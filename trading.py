from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import glob
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
from market import EveMarketMetrics

import sheets as sh
from sheets import service_login

from universe import UniverseLookup
from universe import ItemFactory
from universe import EntityFactory

from weighted_series import WeightedSeriesMetrics

r0 = requester
r = authed_requester


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
entity = EntityFactory(items, universe)
blueprints = BlueprintLookup(items, entity)
ua = UserAssets(r, "Mola Pavonis")

sheets_client = service_login("service-account.json")
eve_trading_sheet = (
    "https://docs.google.com/spreadsheets/d/"
    "1gbsmO3Gl1qBk8uEDaZOIiVNefPdhxDWIN8T0KbzSlKs"
)


names_for_sale = [
    "Small Signal Focusing Kit I",
    "Small Polycarbon Engine Housing I",
    "Small Hyperspatial Velocity Optimizer I",
    "Medium Signal Focusing Kit I",
    "Medium Polycarbon Engine Housing I",
    "Medium Hyperspatial Velocity Optimizer I",
    "Large Signal Focusing Kit I",
    "Large Polycarbon Engine Housing I",
    "Large Hyperspatial Velocity Optimizer I",
    "Large Cap Battery I",
    "Small Low Friction Nozzle Joints I",
    "Small Auxiliary Thrusters I",
    "Medium Low Friction Nozzle Joints I",
    "Medium Auxiliary Thrusters I",
    "Large Low Friction Nozzle Joints I",
    "Large Auxiliary Thrusters I",
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
yona_core = entity.from_name(
    "Yona II - Core Complexion Inc. Factory",
)

order_fetcher = OrderFetcher(universe, disk_cache="orders1", expire=600)

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
mfg_yona = MfgMarket(
    Industry(universe, blueprints),
    order_fetcher,
    mfg_station=stacmon_fed,
    accounting_level=3,
)


def minutes_ago(n):
    return (
        datetime.datetime.now() -
        datetime.timedelta(minutes=n)
    ).timestamp()


class MerchManager:

    save_date_format = "%Y-%m-%dT%H%M"

    @classmethod
    def save_tpl(cls):
        return "merch/{key}.merch"

    @classmethod
    def from_multilookup(cls, mfg, entities, save=True, threads=4):

        def _total_cost(entity):
            result = None
            name = entity.name
            print(f"{name}...")
            try:
                result = mfg.total_cost_with_ingredient_prices(entity)
                print(f"{name}: {result['profit']}")
            except ValueError as err:
                print(f"{name}: {err}")
            return result

        with ThreadPoolExecutor(max_workers=threads) as exe:
            results = list(exe.map(_total_cost, entities))

        merch = dict(zip(entities, results))

        if save:
            tpl = cls.save_tpl()
            now = datetime.datetime.now().strftime(cls.save_date_format)
            path = tpl.format(key=now)
            with open(path, "wb") as f:
                pickle.dump(merch, f)

        return cls(merch)

    @classmethod
    def get_saves(cls):
        tpl = cls.save_tpl()
        entries = sorted(glob.glob(tpl.format(key="*")), reverse=True)
        timestamps = [
            datetime.datetime.strptime(
                entry,
                tpl.format(key=cls.save_date_format),
            ).timestamp()
            for entry in entries
        ]
        return list(zip(timestamps, entries))

    @classmethod
    def most_recent_save(cls):
        tpl = cls.save_tpl()
        return max(glob.glob(tpl.format(key="*")))

    @classmethod
    def latest_save_before(cls, before_ts):
        saves = cls.get_saves()
        return next(
            (path for (ts, path) in saves if ts <= before_ts),
            None,
        )

    @classmethod
    def latest_save_days_back(cls, days_back):
        cutoff = datetime.datetime.now() - datetime.timedelta(days=days_back)
        return cls.latest_save_before(cutoff.timestamp())

    @classmethod
    def from_save(cls, path):
        with open(path, "rb") as f:
            merch = pickle.load(f)
            return cls(merch)

    @classmethod
    def latest(cls, before=None):
        if before is None:
            before = (
                datetime.datetime.now() + datetime.timedelta(minutes=1)
            ).timestamp()
        path = cls.latest_save_before(before)
        if not path:
            raise RuntimeError(f"No save available before {before}")
        return cls.from_save(path)

    def __init__(self, merch):
        self.merch = merch

    def by_profit(self):
        return sorted(
            [(v["profit"] if v else 0, k) for (k, v) in self.merch.items()],
            key=lambda x: x[0],
            reverse=True,
        )

    @property
    def numbering(self):
        return {
            i: k for (i, k) in enumerate(self.merch.keys(), start=1)
        }

    @property
    def reversed_numbering(self):
        return {v: k for (k, v) in self.numbering.items()}

    def profits(self):
        entries = self.by_profit()

        bins = [1800, 1500, 1200, 1000, 800, 500, 100, 50, 0]

        for (up, down) in zip(bins, bins[1:]):
            print(f"# Over {down}k profit")
            if not entries:
                print("# (none)")
            for (p, e) in entries:
                i = self.reversed_numbering[e]
                if down*1000 < p <= up*1000:
                    print(f"{i}) {p} | {e}")
            print("")

    def ls(self):
        pprint(self.numbering)

    def view(self, i):
        pprint(self.merch[self.numbering[i]])


def recipe_grid(blueprints, output_ids, input_ids):
    result = []

    for outp in output_ids:
        ing_triples = blueprints.ingredients(entity.from_id(outp)).triples()
        ing_dict = {
            entity.id: quantity for (_, quantity, entity) in ing_triples
        }

        result.append([ing_dict.get(int(inp), 0) for inp in input_ids])

    return result


spreadsheet = sheets_client.open_by_url(eve_trading_sheet)
recipe_sheet = spreadsheet.worksheet("Recipes")
product_sheet = spreadsheet.worksheet("Products")
ingredient_sheet = spreadsheet.worksheet("Ingredients")


REGIONS = entity.from_names(
    "Sinq Laison",
    "The Forge",
    "Verge Vendor",
    "Placid",
    "Essence",
)


p10 = WeightedSeriesMetrics.percentile(10)
p20 = WeightedSeriesMetrics.percentile(20)
p80 = WeightedSeriesMetrics.percentile(80)
p90 = WeightedSeriesMetrics.percentile(90)
average = WeightedSeriesMetrics.average
minimum = WeightedSeriesMetrics.minimum
maximum = WeightedSeriesMetrics.maximum


def relevant_sell(entity):
    orders = order_fetcher.get_for_regions(entity, REGIONS)
    return EveMarketMetrics.as_series(
        EveMarketMetrics.filter_sell(orders),
    )


def relevant_buy(entity):
    orders = order_fetcher.get_for_regions(entity, REGIONS)
    return EveMarketMetrics.as_series(
        EveMarketMetrics.filter_buy(orders),
    )


def dodixie_sell(entity):
    orders = order_fetcher.get_for_station(entity, dodixie_fed)
    return EveMarketMetrics.as_series(
        EveMarketMetrics.filter_location(
            dodixie_fed,
            EveMarketMetrics.filter_sell(orders),
        ),
    )


def dodixie_buy(entity):
    orders = order_fetcher.get_for_station(entity, dodixie_fed)
    return EveMarketMetrics.as_series(
        EveMarketMetrics.filter_location(
            dodixie_fed,
            EveMarketMetrics.filter_buy(orders),
        ),
    )


def update_sheet(spreadsheet, ua, entity, mm=None):
    product_sheet = spreadsheet.worksheet("Products")
    ingredient_sheet = spreadsheet.worksheet("Ingredients")

    product_ids = product_sheet.get_values("ProductIDs")
    ingredient_ids = ingredient_sheet.get_values("IngredientIDs")

    def map_product_ids_to_col(output_col, func):
        return product_sheet.update(
            output_col,
            sh.threadmap_col_range(func, product_ids),
        )

    def map_ingredient_ids_to_col(output_col, func):
        return ingredient_sheet.update(
            output_col,
            sh.threadmap_col_range(func, ingredient_ids),
        )

    # Update stock of products and ingredients
    print("Reading inventory...")
    tots = ua.total_quantities()
    print("Updating product stock...")
    map_product_ids_to_col("ProductStock", lambda x: tots.get(int(x), 0))
    print("Updating ingredient stock...")
    map_ingredient_ids_to_col("IngredientStock", lambda x: tots.get(int(x), 0))

    # Update order volume of products
    print("Reading orders...")
    order_volume = {}
    for order in ua.orders():
        ent = order["type_id"]
        order_volume[ent] = order_volume.get(ent, 0) + order["volume_remain"]
    print("Updating product order volume...")
    map_product_ids_to_col("ProductOrderVolume", lambda x: order_volume.get(int(x), 0))

    # Update sell p10, dodixie sell p10 products
    print("Updating product relevant sell...")
    map_product_ids_to_col(
        "ProductSellP10",
        lambda x: p10(relevant_sell(entity.from_id(x).entity)),
    )
    print("Updating product dodixie sell...")
    map_product_ids_to_col(
        "ProductDodixieSellP10",
        lambda x: p10(dodixie_sell(entity.from_id(x).entity)),
    )

    # Update sell p20, dodixie sell p20, dodixie buy p90 ingredients
    print("Updating ingredient dodixie sell...")
    map_ingredient_ids_to_col(
        "IngredientDodixieSellP20",
        lambda x: p20(dodixie_sell(entity.from_id(x).entity)),
    )
    print("Updating ingredient dodixie buy...")
    map_ingredient_ids_to_col(
        "IngredientDodixieBuyP90",
        lambda x: p90(dodixie_buy(entity.from_id(x).entity)),
    )

    if mm is not None:
        map_product_ids_to_col(
            "ProductCraftCost",
            lambda x: mm.merch.get(entity.from_id(x).entity, {}).get("total", -1)
        )
