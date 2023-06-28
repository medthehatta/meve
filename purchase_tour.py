import itertools
import pickle

import diskcache
import networkx as nx
from requests.exceptions import HTTPError

from hxxp import DefaultHandlers

from astar import state_astar

from timer import Timer
from util import upper_triangle


cache = diskcache.Cache("meve_cache")

_json = DefaultHandlers.raise_or_return_json


class Travel:

    def __init__(self, dest, cost):
        self.dest = dest
        self.cost = cost

    def __repr__(self):
        return f"move {self.dest}"

    def format(self, ids_to_names):
        (system, station) = self.dest
        station_name = ids_to_names.from_id(station).name
        return f"move to [{station_name}] (opp. cost {self.cost:,})"


class Purchase:

    def __init__(self, what, where, cost):
        self.what = what
        self.where = where
        self.cost = cost

    def __repr__(self):
        return f"buy {self.what} {self.where}"

    def format(self, ids_to_names):
        (amount, item_id) = self.what
        (system, station) = self.where
        item_name = ids_to_names.from_id(item_id).name
        station_name = ids_to_names.from_id(station).name
        return (
            f"buy "
            f"{amount} [{item_name}] "
            f"from [{station_name}] "
            f"for [{self.cost/amount:,} ea]"
        )


class State:

    def __init__(
        self,
        graph,
        markets,
        position,
        required=None,
        move_cost_per_second=4160,
    ):
        self.graph = graph
        self.markets = markets
        self.position = position
        self.required = required if required is not None else set([])
        self.move_cost_per_second = move_cost_per_second

    def __eq__(self, other):
        return (
            self.position == other.position
            and self.required == other.required
        )

    def __hash__(self):
        return hash((self.position, tuple(sorted(self.required))))

    def __repr__(self):
        return f"{self.position}={self.required}"

    def transition(self, x):
        defaults = {
            "graph": self.graph,
            "markets": self.markets,
            "move_cost_per_second": self.move_cost_per_second,
            "position": self.position,
            "required": self.required,
        }
        if isinstance(x, Purchase):
            return type(self)(
                **{
                    **defaults,
                    "required": self.required.difference({x.what}),
                },
            )

        elif isinstance(x, Travel):
            return type(self)(
                **{
                    **defaults,
                    "position": x.dest,
                },
            )

        else:
            raise ValueError(f"Unknown transition for {type(self)}: {x}")

    def neighbors(self):
        # Purchase from current station
        for (amount, item) in self.required:
            # Skip if this station doesn't have the item
            if (
                self.position not in self.markets
                or item not in self.markets[self.position]
            ):
                continue
            cost = amount * self.markets[self.position][item]["price"]
            transition = Purchase((amount, item), self.position, cost)
            state = self.transition(transition)
            yield (transition, state, cost)

        # Move to an adjacent station
        for n in self.graph.neighbors(self.position):
            jump_seconds = self.graph.get_edge_data(self.position, n)["weight"]
            cost = jump_seconds*self.move_cost_per_second
            transition = Travel(n, cost)
            state = self.transition(transition)
            yield (transition, state, cost)

    def heuristic(self, goal):
        # This heuristic should just be a sum of the minimum costs remaining
        # for everything, with jump cost omitted.
        #
        # For now the search is so quick I don't even need a heuristic, so we
        # just return 0 which makes this do a full best-first search.
        #
        return 0


# FIXME: should maybe move this
def load_system_graph(path):
    graph = nx.Graph()
    with open(path, "rb") as f:
        graph_data = pickle.load(f)
    for edge in graph_data["edges"]:
        graph.add_edge(*edge)
    return graph


def get_sell_orders(r, region_id, type_id=None, page=1):
    params = {
        "order_type": "sell",
        "page": page,
    }

    if type_id is not None:
        params["type_id"] = type_id

    return _json(r.request("GET", f"/markets/{region_id}/orders", params=params))


def iter_sell_orders(r, region_id, type_id=None):
    page = 1
    while True:
        try:
            yield from get_sell_orders(
                r,
                region_id=region_id,
                type_id=type_id,
                page=page,
            )
            page += 1
        except HTTPError:
            return


_routes = {}


@cache.memoize(ignore={0})
def get_route(system_graph, first, last):
    if (first, last) in _routes:
        return _routes[(first, last)]
    elif (last, first) in _routes:
        return reversed(_routes[(last, first)])
    else:
        _routes[(first, last)] = nx.shortest_path(system_graph, first, last)
        return _routes[(first, last)]


def markets_inventories(requester, region_ids, item_ids):
    market_entries = itertools.chain.from_iterable(
        itertools.chain.from_iterable(
            iter_sell_orders(requester, region_id, int(item_id))
            for region_id in region_ids
        )
        for item_id in item_ids
    )

    markets = set([])
    inventories = {}

    for entry in market_entries:
        what = entry["type_id"]
        where = (entry["system_id"], entry["location_id"])

        markets.add(where)

        if where not in inventories:
            inventories[where] = {what: [entry]}
        elif what not in inventories[where]:
            inventories[where][what] = [entry]
        else:
            inventories[where][what].append(entry)

    # Take the best result for each item for each station
    for where in inventories:
        for what in inventories[where]:
            inventories[where][what] = min(
                inventories[where][what],
                key=lambda x: x["price"],
            )

    return (markets, inventories)


def compute_graph(system_graph, markets):
    in_system_travel_seconds = 30
    jump_seconds = 60

    system_set = {}
    for (sys, mkt) in markets:
        system_set[sys] = system_set.get(sys, []) + [(sys, mkt)]

    g = nx.Graph()
    for (m1, m2) in upper_triangle(markets):
        (sys1, mkt1) = m1
        (sys2, mkt2) = m2

        # In-system travel is a special case
        if sys1 == sys2:
            g.add_edge(m1, m2, weight=in_system_travel_seconds)
            continue

        # Travel between systems
        route = iter(get_route(system_graph, sys1, sys2))

        # Routes can include other systems than just sys1 and sys2, so
        # this isn't necessarily telling us directly about the
        # connectivity between sys1 and sys2.  We need to walk the
        # route, counting jumps through systems we don't care about,
        # until whenever we encounter one of the systems we care about
        # (systems in system_set) we record that edge length.
        #
        # That is, a single route between two systems might give us
        # multiple edges in our graph of nodes we actually care about.
        #
        # Because these routes are guaranteed by get_route() to be the
        # SHORTEST routes, we will never clobber an edge with a
        # different weight, it will always be the minimum one between
        # those nodes.
        #
        jumps = []

        start = next(route)
        w = 1
        for s in route:
            if s in system_set:
                jumps.append((start, s, w))
                w = 1
                start = s
            else:
                w += 1

        # We have found jump connectivity between the systems, but each
        # system could have multiple markets.  We need to add graph
        # edges for each market pair between the systems
        for (left, right, weight) in jumps:
            left_markets = system_set[left]
            right_markets = system_set[right]
            for ml in left_markets:
                for mr in right_markets:
                    g.add_edge(ml, mr, weight=weight*jump_seconds)

    return g


def optimize_purchase(
    requester,
    system_graph,
    items,
    desired,
    region_ids,
    start_position,
    end_position=None,
    timer=None,
    cost_per_second=4160,
):
    timer = timer or Timer(trace=True)
    end_position = end_position or start_position

    timer.checkpoint("Reformat desired entities")
    required = {
        (amount, items.from_terms(fuzzy_name).id)
        for (amount, fuzzy_name) in desired
    }
    required_ids = {item_id for (_, item_id) in required}

    timer.checkpoint("Fetch relevant market data")
    (markets, inventories) = markets_inventories(
        requester,
        region_ids,
        required_ids,
    )

    # Make sure we have the start position in here so we can plot a route from
    # there, ditto end position
    markets.add(start_position)
    markets.add(end_position)

    timer.checkpoint("Compute graph")
    g = compute_graph(system_graph, markets)

    timer.checkpoint("Set up optimization problem")
    initial = State(
        g,
        inventories,
        position=start_position,
        required=required,
        move_cost_per_second=cost_per_second,
    )
    final = State(
        g,
        inventories,
        position=end_position,
        required=set([]),
        move_cost_per_second=cost_per_second,
    )

    timer.checkpoint("Perform A*")
    (total_cost, procedure) = state_astar(
        initial,
        final,
        State.neighbors,
        State.heuristic,
    )

    timer.checkpoint("Complete")
    return (total_cost, procedure)

