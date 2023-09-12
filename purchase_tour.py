import itertools
import pickle

import diskcache
import networkx as nx
from requests.exceptions import HTTPError
from cytoolz import groupby
from cytoolz import topk

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
        best_prices,
        position,
        required=None,
        move_cost_per_second=4160,
    ):
        self.graph = graph
        self.markets = markets
        self.best_prices = best_prices
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
            "best_prices": self.best_prices,
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
                or self.markets[self.position][item]["volume_remain"] < amount
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
        return 0
        # This performs WORSE than just not putting a heuristic in
        min_buy_cost = sum(
            amount * self.best_prices[item]
            for (amount, item) in self.required
        )
        min_path = nx.shortest_path(self.graph, self.position, goal.position)
        path_seconds = sum(
            self.graph.get_edge_data(a, b)["weight"]
            for (a, b) in zip(min_path, min_path[1:])
        )
        travel_cost = path_seconds*self.move_cost_per_second
        return min_buy_cost + travel_cost


# FIXME: should maybe move this
def load_system_graph(path):
    graph = nx.Graph()
    with open(path, "rb") as f:
        graph_data = pickle.load(f)
    for edge in graph_data["edges"]:
        graph.add_edge(*edge)
    return graph


def get_orders(r, query, region_id, type_id=None, page=1):
    params = {
        **query,
        "page": page,
    }

    if type_id is not None:
        params["type_id"] = type_id

    return _json(r.request("GET", f"/markets/{region_id}/orders", params=params))


def iter_orders(r, query, region_id, type_id=None):
    page = 1
    while True:
        try:
            yield from get_orders(
                r,
                query,
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


def orders_for_item(requester, region_ids, item_id):
    return itertools.chain.from_iterable(
        iter_orders(
            requester,
            {"order_type": "all"},
            region_id,
            int(item_id),
        )
        for region_id in region_ids
    )


def orders_in_regions(requester, region_ids, item_ids):
    return itertools.chain.from_iterable(
        orders_for_item(requester, region_ids, item_id)
        for item_id in item_ids
    )


def orders_for_item_at_location(requester, universe, item, location):
    region = universe.chain(
        location, "station", "system", "constallation", "region",
    )
    return (
        entry for entry in orders_for_item(requester, [region.id], item.id)
        if entry["location_id"] == location.id
    )


def orders_for_item_in_system(requester, universe, item, system):
    region = universe.chain(
        system, "system", "constellation", "region",
    )
    return (
        entry for entry in orders_for_item(requester, [region.id], item.id)
        if entry["system_id"] == system.id
    )


def orders_by_item(market_entries):
    orders = {}

    for entry in market_entries:
        what = entry["type_id"]
        where = entry["location_id"]

        if what not in orders:
            orders[what] = {where: [entry]}
        elif where not in orders[what]:
            orders[what][where] = [entry]
        else:
            orders[what][where].append(entry)

    return orders


def orders_by_location(market_entries):
    orders = {}

    for entry in market_entries:
        what = entry["type_id"]
        where = entry["location_id"]

        if where not in orders:
            orders[where] = {what: [entry]}
        elif what not in orders[where]:
            orders[where][what] = [entry]
        else:
            orders[where][what].append(entry)

    return orders


def markets_inventories(requester, region_ids, item_ids):
    market_entries = itertools.chain.from_iterable(
        itertools.chain.from_iterable(
            iter_orders(
                requester,
                {"order_type": "sell"},
                region_id,
                int(item_id),
            )
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


def min_inventory_prices(inventory):
    min_prices = {}
    for location in inventory:
        for item in inventory[location]:
            if (
                item not in min_prices or
                inventory[location][item]["price"] < min_prices[item]
            ):
                min_prices[item] = inventory[location][item]["price"]
    return min_prices


def item_to_location_candidates(
    universe,
    orders,
    top=2,
    order_type="sell",
    reverse=False,
):
    by_item = groupby(lambda x: x["type_id"], orders)

    best_by_location = {}

    for (item, item_entries) in by_item.items():
        by_station = groupby(lambda x: x["location_id"], item_entries)
        for (location, location_entries) in by_station.items():
            if item not in best_by_location:
                best_by_location[item] = {}
            best_by_location[item][location] = list(
                topk(
                    top,
                    [
                        entry for entry in location_entries
                        if entry["is_buy_order"] == (order_type == "buy")
                    ],
                    key=lambda x: x["price"] if reverse else -x["price"],
                )
            )

    sorted_by_location = {}

    for item in best_by_location:
        if item not in sorted_by_location:
            sorted_by_location[item] = {}

        for (location, candidates) in best_by_location[item].items():
            sorted_candidates = sorted(
                candidates,
                key=lambda x: x["price"],
                reverse=reverse,
            )
            sorted_by_location[item][location] = sorted_candidates

        sorted_locations = sorted(
            [
                (k, v) for (k, v) in sorted_by_location[item].items()
                if v
            ],
            key=lambda x: x[1][0]["price"],
            reverse=reverse,
        )

        sorted_by_location[item] = sum([v for (k, v) in sorted_locations], [])

    return sorted_by_location


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
    required_tally = {}
    for (amount, fuzzy_name) in desired:
        item_id = items.from_terms(fuzzy_name).id
        required_tally[item_id] = required_tally.get(item_id, 0) + amount
    required = {(amount, id_) for (id_, amount) in required_tally.items()}
    required_ids = {item_id for (_, item_id) in required}

    timer.checkpoint("Fetch relevant market data")
    (markets, inventories) = markets_inventories(
        requester,
        region_ids,
        required_ids,
    )

    best_prices = min_inventory_prices(inventories)

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
        best_prices=best_prices,
        position=start_position,
        required=required,
        move_cost_per_second=cost_per_second,
    )
    final = State(
        g,
        inventories,
        best_prices=best_prices,
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
        trace=timer.checkpoint,
    )

    timer.checkpoint("Complete")
    return (total_cost, procedure)

