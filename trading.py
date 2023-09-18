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
    "Small Auxiliary Thrusters I",
    "Small Cargohold Optimization I",
    "Small Hyperspatial Velocity Optimizer I",
    "Small Low Friction Nozzle Joints I",
    "Small Polycarbon Engine Housing I",
    "Small Signal Focusing Kit I",
    # new
    "Core Probe Launcher I",
]
merchandise = entity.from_name_seq(names_for_sale)

merch = merchandise[0].entity

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


order_fetcher = OrderFetcher(universe, disk_cache="orders1", expire=200)

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


def multilookup(mfg, entities):
    foo = {}
    for entity in entities:
        name = entity.name
        print(f"{name}...")
        try:
            foo[name] = mfg.total_cost_with_ingredient_prices(entity)
            print(f"{name}: {foo[name]['profit']}")
        except ValueError as err:
            print(f"{name}: {err}")
    return foo
