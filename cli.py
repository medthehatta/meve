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
from purchase_tour import orders_in_regions
from purchase_tour import item_to_location_candidates
from universe import ItemFactory
from universe import UniverseLookup
from universe import station_lookup
from industry import BlueprintLookup


TIME_COSTS = {
    "chill": 900,
    "normal": 1630,
    "sweaty": 4160,
}

DEFAULT_REGION_NAMES = [
    "Verge Vendor",
    "Placid",
    "Essence",
    "Sinq Laison",
]

DEFAULT_START_STATION_NAME = "Cistuvaert V - AIR Laboratories"
DEFAULT_END_STATION_NAME = "Cistuvaert V - AIR Laboratories"

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
        match_no_num = re.search(r"^(.+)\s*$", line)
        if match:
            count = float(match.group(1))
            item = match.group(2).lower()
            components[item] = components.get(item, 0) + count
        elif match_no_num:
            count = 1
            item = match_no_num.group(1).lower()
            components[item] = components.get(item, 0) + count
        else:
            print(f"No match: '{line}'")
    parsed = {(count, item) for (item, count) in components.items()}
    return parsed


@click.group()
def cli():
    """Some EVE helpers."""


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
)
@click.option(
    "-e",
    "--end-station",
    "--end",
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

    if start_station is None and end_station is None:
        start_position = station_lookup(universe, DEFAULT_START_STATION_NAME)
        end_position = station_lookup(universe, DEFAULT_END_STATION_NAME)
    elif start_station is None:
        end_position = station_lookup(universe, DEFAULT_END_STATION_NAME)
        start_position = end_position
    elif end_station is None:
        start_position = station_lookup(universe, DEFAULT_START_STATION_NAME)
        end_position = start_position
    else:
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
    "-t",
    "--order-type",
    default="all",
)
@click.option(
    "-r",
    "--region",
    default=DEFAULT_REGION_NAMES,
    multiple=True,
)
@click.option(
    "-l",
    "--locations",
    default=0,
)
@click.option(
    "--reverse",
    is_flag=True,
)
@click.argument("items", type=click.File("r"))
def orders(region, order_type, locations, reverse, items):
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

    market_entries = orders_in_regions(requester, region_ids, required_ids)

    if locations:
        entries_ids = item_to_location_candidates(
            universe,
            market_entries,
            top=locations,
            reverse=reverse,
            order_type=order_type,
        )
        entries = {
            universe.from_id(k).name: [
                {
                    "station": universe.from_id(v["location_id"]).name,
                    "price": v["price"],
                    "volume": v["volume_remain"],
                }
                for v in vv
            ]
            for (k, vv) in entries_ids.items()
        }
        print(json.dumps(entries))
    else:
        for entry in market_entries:
            if order_type == "buy" and not entry["is_buy_order"]:
                continue
            elif order_type == "sell" and entry["is_buy_order"]:
                continue

            entry1 = {
                "item": universe.from_id(entry["type_id"]).name,
                "system": universe.from_id(entry["system_id"]).name,
                "station": universe.from_id(entry["location_id"]).name,
                "price": entry["price"],
                "volume": entry["volume_remain"],
                "kind": "buy" if entry["is_buy_order"] else "sell",
            }
            print(json.dumps(entry1))


@cli.command()
@click.option("--oneline", is_flag=True)
@click.argument("item")
def blueprint(oneline, item):
    desired = next(iter(parse_recipe_lines([f"1 {item}"])))

    requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
    items = ItemFactory(requester, "types.json")
    blueprints = BlueprintLookup(items)

    desired_id = items.from_terms(desired[-1]).id

    ingredients = blueprints.ingredients(entity_id=desired_id)

    if oneline:
        print(ingredients)
    else:
        print(ingredients.pretty_components())


if __name__ == "__main__":
    cli()
