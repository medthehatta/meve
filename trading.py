import itertools
import datetime
from pprint import pprint
import pickle

from api_access import requester
from api_access import authed_requester

from user_data import UserAssets

from industry import BlueprintLookup
from industry import Industry
from industry import MfgMarket

from market import OrderFetcher

from universe import UniverseLookup
from universe import ItemFactory
from universe import EntityFactory

r0 = requester
r = authed_requester


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
entity = EntityFactory(items, universe)
blueprints = BlueprintLookup(items, entity)
ua = UserAssets(r, "Mola Pavonis")


names_for_sale = [
    # over 100k profit per unit
    "Small Signal Focusing Kit I",
    "Small Polycarbon Engine Housing I",

    # over 50k profit per unit
    "Small Hyperspatial Velocity Optimizer I",
    "Large Cap Battery I",

    # still profitable
    "Small Low Friction Nozzle Joints I",
    "Small Auxiliary Thrusters I",
    "Core Probe Launcher I",

    # not profitable
    #"Small Cargohold Optimization I",
]
merch = entity.from_name_seq(names_for_sale)

industry = Industry(universe, blueprints)

dodixie_fed = entity.from_name(
    "Dodixie IX - Moon 20 - Federation Navy Assembly Plant",
)
jita_44 = entity.from_name(
    "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
)
alentene_roden = entity.from_name(
    "Alentene VI - Moon 6 - Roden Shipyards Warehouse",
)
stacmon_fed = entity.from_name(
    "Stacmon V - Moon 9 - Federation Navy Assembly Plant",
)


order_fetcher = OrderFetcher(universe, disk_cache="orders1", expire=300)

mfg_dodixie = MfgMarket(
    Industry(universe, blueprints),
    order_fetcher,
    mfg_station=dodixie_fed,
    accounting_level=3,
)
mfg_jita = MfgMarket(
    Industry(universe, blueprints),
    order_fetcher,
    mfg_station=jita_44,
    accounting_level=3,
)
mfg_stacmon = MfgMarket(
    Industry(universe, blueprints),
    order_fetcher,
    mfg_station=stacmon_fed,
    accounting_level=3,
)


def multilookup(mfg, entities, save=True):
    foo = {}
    for entity in entities:
        name = entity.name
        print(f"{name}...")
        try:
            foo[name] = mfg.total_cost_with_ingredient_prices(entity)
            print(f"{name}: {foo[name]['profit']}")
        except ValueError as err:
            print(f"{name}: {err}")
    if save:
        now = datetime.datetime.now().strftime("%Y-%m-%dT%H%M")
        with open(f"merch/{now}.merch", "wb") as f:
            pickle.dump(foo, f)
    return foo


# my_merch = multilookup(mfg_dodixie, merch)
with open("merch/2023-09-21T0758.merch", "rb") as f:
    my_merch = pickle.load(f)

numbering = {i: k for (i, k) in enumerate(my_merch.keys(), start=1)}
r_numbering = {v: k for (k, v) in numbering.items()}


def ls():
    profits(my_merch)


def view(i):
    pprint(my_merch[numbering[i]])


def profits(merch_dict):
    entries = sorted([(v["profit"], k) for (k, v) in merch_dict.items()], reverse=True)

    bins = [500, 100, 50, 0]

    for (up, down) in zip(bins, bins[1:]):
        print(f"# Over {down}k profit")
        for (p, e) in entries:
            i = r_numbering[e]
            if down*1000 < p <= up*1000:
                print(f"{i}) {p} | {e}")
        print("")
