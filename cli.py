import itertools
import json
import re
import json

import click
from cytoolz import groupby

from authentication import EmptyToken
from hxxp import Requester
from purchase_tour import optimize_purchase
from purchase_tour import Purchase
from purchase_tour import Travel
# FIXME: should maybe move this
from purchase_tour import load_system_graph
from purchase_tour import markets_inventories
from purchase_tour import iter_sell_orders
from universe import ItemFactory
from universe import UniverseLookup
from universe import station_lookup


TIME_COSTS = {
    "chill": 900,
    "normal": 1630,
    "sweaty": 4160,
}

DEFAULT_REGION_NAMES = [
    "Verge Vendor",
    "Placid",
    "Essence",
]

DEFAULT_START_STATION_NAME = "Loes V - Moon 19 - Roden Shipyards Warehouse"

DEFAULT_END_STATION_NAME = "Loes V - Moon 19 - Roden Shipyards Warehouse"

DEFAULT_SWEAT_LEVEL = "medium"


def parse_recipe_lines(lines):
    components = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        match = re.search(r"^(\d+(?:[.]\d*)?)\s+(.+)\s*$", line)
        if match:
            count = float(match.group(1))
            item = match.group(2).lower()
            components[item] = components.get(item, 0) + count
        else:
            print(f"No match: '{line}'")
    parsed = {(count, item) for (item, count) in components.items()}
    return parsed


@click.group()
def cli():
    """Plot routes through New Eden to buy materials."""


@cli.command()
@click.option("-b", "--brief", is_flag=True)
@click.argument("kind")
@click.argument("name")
def universe(name, brief, kind):
    requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
    universe = UniverseLookup(requester)
    if brief:
        print(universe.from_name(name))
    else:
        print(json.dumps(universe.details(kind, name=name)))


@cli.command()
@click.option("-b", "--brief", is_flag=True)
@click.argument("terms")
def item(terms, brief):
    requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
    universe = UniverseLookup(requester)
    items = ItemFactory(requester, "types.json")
    item = items.from_terms(terms)
    if brief:
        print(item)
    else:
        print(json.dumps(universe.details("type", entity_id=item.id)))


@cli.command()
@click.option(
    "-s",
    "--start-station",
    "--start",
    default=DEFAULT_START_STATION_NAME,
)
@click.option(
    "-e",
    "--end-station",
    "--end",
    default=DEFAULT_END_STATION_NAME,
)
@click.option(
    "-r",
    "--region",
    default=DEFAULT_REGION_NAMES,
    multiple=True,
)
@click.option(
    "--opportunity-cost-per-second",
    "--opp-cost",
    type=float,
)
@click.option(
    "-w",
    "--sweat-level",
    type=click.Choice(TIME_COSTS.keys()),
    default="normal",
)
@click.argument("items", type=click.File("r"))
def plot(
    start_station,
    end_station,
    region,
    opportunity_cost_per_second,
    sweat_level,
    items,
):
    # Better name for the variable
    region_names = region

    if opportunity_cost_per_second is not None:
        cost_per_second = opportunity_cost_per_second
    else:
        cost_per_second = TIME_COSTS[sweat_level]

    desired = parse_recipe_lines(items)

    requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
    universe = UniverseLookup(requester)

    start_position = station_lookup(universe, start_station)
    end_position = station_lookup(universe, end_station)

    regions = [
        universe.from_name(region_name).id for region_name in region_names
    ]

    items = ItemFactory(requester, "types.json")

    # FIXME: This graph has particular regions baked in!
    system_graph = load_system_graph("graph.pkl")

    (total_cost, procedure) = optimize_purchase(
        requester=requester,
        system_graph=system_graph,
        items=items,
        desired=desired,
        region_ids=regions,
        start_position=start_position,
        end_position=end_position,
        cost_per_second=cost_per_second,
    )

    costs = {
        kind: sum(entry.cost for entry in entries)
        for (kind, entries)
        in groupby(lambda x: type(x).__name__, procedure).items()
    }

    print("")

    for (kind, total) in costs.items():
        print(f"{kind.lower()} cost: {total:,}")

    print("")

    for entry in procedure:
        if isinstance(entry, Purchase):
            print(entry.format(universe))


@cli.command()
@click.option(
    "-r",
    "--region",
    default=DEFAULT_REGION_NAMES,
    multiple=True,
)
@click.argument("items", type=click.File("r"))
def sell_orders(region, items):
    # Better name for the variable
    region_names = region

    desired = parse_recipe_lines(items)

    requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
    universe = UniverseLookup(requester)

    region_ids = [
        universe.from_name(region_name).id for region_name in region_names
    ]

    items = ItemFactory(requester, "types.json")

    required = {
        (amount, items.from_terms(fuzzy_name).id)
        for (amount, fuzzy_name) in desired
    }
    required_ids = {item_id for (_, item_id) in required}

    market_entries = itertools.chain.from_iterable(
        itertools.chain.from_iterable(
            iter_sell_orders(requester, region_id, int(item_id))
            for region_id in region_ids
        )
        for item_id in required_ids
    )

    for entry in market_entries:
        entry1 = {
            "item": universe.from_id(entry["type_id"]).name,
            "system": universe.from_id(entry["system_id"]).name,
            "station": universe.from_id(entry["location_id"]).name,
            "price": entry["price"],
            "volume": entry["volume_remain"],
        }
        print(json.dumps(entry1))


if __name__ == "__main__":
    cli()
