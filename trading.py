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

from market import OrderCalc
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
    def from_multilookup(cls, mfg, entities, save=True, threads=4, recurse=None):

        def _total_cost(entity):
            result = None
            name = entity.name
            print(f"{name}...")
            try:
                result = mfg.total_cost_with_ingredient_prices(entity, recurse=recurse)
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


def recipe_grid(blueprints, entities):
    ings = set([])
    rows = []

    for outp in entities:
        ing_triples = blueprints.ingredients(outp).triples()
        ings = ings.union([entity for (_, _, entity) in ing_triples])
        ing_dict = {
            entity.id: quantity for (_, quantity, entity) in ing_triples
        }
        rows.append({"Product": outp.name, "ID": outp.id, **ing_dict})

    all_ings = list(ings)
    yield (["", "Ingredient"] + [x.name for x in all_ings])
    yield (["Product", "ID"] + [x.id for x in all_ings])
    for row in rows:
        yield ([row["Product"], row["ID"]] + [row.get(x.id, 0) for x in all_ings])


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


p1 = lambda x: WeightedSeriesMetrics.percentile(1, x) or None
p10 = lambda x: WeightedSeriesMetrics.percentile(10, x) or None
p20 = lambda x: WeightedSeriesMetrics.percentile(20, x) or None
p80 = lambda x: WeightedSeriesMetrics.percentile(80, x) or None
p90 = lambda x: WeightedSeriesMetrics.percentile(90, x) or None
average = lambda x: WeightedSeriesMetrics.average(x) or None
minimum = lambda x: WeightedSeriesMetrics.minimum(x) or None
maximum = lambda x: WeightedSeriesMetrics.maximum(x) or None


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


def station_sell(station, entity):
    orders = order_fetcher.get_for_station(entity, station)
    return EveMarketMetrics.as_series(
        EveMarketMetrics.filter_location(
            dodixie_fed,
            EveMarketMetrics.filter_sell(orders),
        ),
    )


def station_buy(station, entity):
    orders = order_fetcher.get_for_station(entity, station)
    return EveMarketMetrics.as_series(
        EveMarketMetrics.filter_location(
            dodixie_fed,
            EveMarketMetrics.filter_buy(orders),
        ),
    )


def update_sheet(spreadsheet, ua, entity, industry, station):
    meta_sheet = spreadsheet.worksheet("Meta")
    product_sheet = spreadsheet.worksheet("Products")
    ingredient_sheet = spreadsheet.worksheet("Ingredients")

    meta_sheet.update("A1", [[station.name]])

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
    tots = ua.aggregate_on_field(
        "quantity",
        (
            asset for asset in ua.assets()
            if asset["location_id"] == station.id
        ),
    )
    print("Updating product stock...")
    map_product_ids_to_col("ProductStock", lambda x: tots.get(int(x) if x else None, 0))
    print("Updating ingredient stock...")
    map_ingredient_ids_to_col("IngredientStock", lambda x: tots.get(int(x) if x else None, 0))

    # Update order volume of products
    print("Reading orders...")
    orders = ua.orders()
    order_volume = ua.aggregate_on_field("volume_remain", orders)
    min_sell = {}
    for x in orders:
        type_id = x["type_id"]
        min_sell[type_id] = (
            min(min_sell[type_id], x["price"]) if type_id in min_sell
            else x["price"]
        )
    print("Updating product order volume...")
    map_product_ids_to_col("ProductOrderVolume", lambda x: order_volume.get(int(x) if x else None, 0))
    print("Updating product sale prices...")
    map_product_ids_to_col("MinOrderPrice", lambda x: min_sell.get(int(x) if x else None, 0))

    # Update craft volume of products
    print("Reading jobs...")
    craft_volume = ua.aggregate_on_field(
        "runs",
        ua.jobs(),
        type_field="product_type_id",
    )
    print("Updating product job volume...")
    map_product_ids_to_col("ProductCraftVolume", lambda x: craft_volume.get(int(x) if x else None, 0))

    # Update sell p10, station sell p10 products
    print("Updating product relevant sell...")
    map_product_ids_to_col(
        "ProductSellP10",
        lambda x: p1(relevant_sell(entity.from_id(x).entity)),
    )
    print("Updating product station sell...")
    map_product_ids_to_col(
        "ProductDodixieSellP10",
        lambda x: p1(station_sell(station, entity.from_id(x).entity)),
    )

    # Update sell p20, station sell p20, station buy p90 ingredients
    print("Updating ingredient station sell...")
    map_ingredient_ids_to_col(
        "IngredientDodixieSellP20",
        lambda x: p20(station_sell(station, entity.from_id(x).entity)),
    )
    print("Updating ingredient station buy...")
    map_ingredient_ids_to_col(
        "IngredientDodixieBuyP90",
        lambda x: p90(station_buy(station, entity.from_id(x).entity)),
    )

    print("Updating ingredient base costs...")
    mp = industry.market_prices()
    map_ingredient_ids_to_col(
        "IngredientBaseCost",
        lambda x: mp["adjusted"][int(x)],
    )


# def reprocess_flip(spreadsheet, ua, entity, station):
#     order_calc = OrderCalc(
#         broker_fee_percent=3,
#         accounting_level=4,
#     )
#     return order_calc.sale_cost(sell_price)


def ore_variants(ore, *variants):
    return [
        " ".join([compression, modifier, ore]).strip()
        for (compression, modifier, ore) in
        itertools.product(
            ["", "compressed", "batch compressed"],
            itertools.chain([""], variants),
            [ore],
        )
    ]

def print_ore_variants(ore, *variants):
    return print("\n".join(ore_variants(ore, *variants)))
