from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import glob
import itertools
import datetime
from pprint import pprint
import pickle

from cytoolz import get
from cytoolz import unique

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

from structure_scraper import StructureScraper

from tracked_map import TrackedMap

from universe import UniverseLookup
from universe import ItemFactory
from universe import EntityFactory
from universe import Entity

from weighted_series import WeightedSeriesMetrics

r0 = requester
r = authed_requester


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
entity = EntityFactory(items, universe)
blueprints = BlueprintLookup(items, entity)
ua = UserAssets(r, "Mola Pavonis")

structs = StructureScraper(entity, ua)

sheets_client = service_login("service-account.json")
eve_trading_sheet = (
    "https://docs.google.com/spreadsheets/d/"
    "1gbsmO3Gl1qBk8uEDaZOIiVNefPdhxDWIN8T0KbzSlKs"
)


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

# k7 = {
#     "entity": Entity(1043661023026, "K7D-II - Mothership Bellicose"),
#     "type": entity.strict.from_name("Keepstar"),
#     "system": entity.strict.from_name("K7D-II"),
# }
# 
# drac = {
#     "entity": Entity(1034323745897, "P-ZMZV - Dracarys Prime"),
#     "type": entity.strict.from_name("Keepstar"),
#     "system": entity.strict.from_name("P-ZMZV"),
# }


order_fetcher = OrderFetcher(
    universe,
    requester,
    authed_requester=authed_requester,
    disk_cache="orders1",
    expire=300,
)

# mfg_dodixie = MfgMarket(
#     Industry(universe, blueprints),
#     order_fetcher,
#     mfg_station=dodixie_fed,
#     accounting_level=3,
# )
# mfg_jita = MfgMarket(
#     Industry(universe, blueprints),
#     order_fetcher,
#     mfg_station=jita_44,
#     accounting_level=3,
# )
# mfg_stacmon = MfgMarket(
#     Industry(universe, blueprints),
#     order_fetcher,
#     mfg_station=stacmon_fed,
#     accounting_level=3,
# )
# mfg_yona = MfgMarket(
#     Industry(universe, blueprints),
#     order_fetcher,
#     mfg_station=stacmon_fed,
#     accounting_level=3,
# )


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


REGIONS = entity.from_names(
    "Sinq Laison",
    "The Forge",
    "Verge Vendor",
    "Placid",
    "Essence",
)

def pctl(k):
    def _pctl(x):
        return WeightedSeriesMetrics.percentile(k, x)
    return _pctl

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


class SheetInterface:

    def __init__(
        self,
        spreadsheet,
        ua,
        entity,
        order_fetcher,
        industry,
        blueprints,
        station,
        structures=None,
    ):
        self.spreadsheet = spreadsheet
        self.ua = ua
        self.entity = entity
        self.order_fetcher = order_fetcher
        self.industry = industry
        self.blueprints = blueprints
        self.station = station
        self._sheets = None
        self._translators = {
            "col": (sh.get_col_range, sh.to_col_range),
            "row": (sh.get_row_range, sh.to_row_range),
        }
        self._default_translators = (lambda x: x, lambda x: x)
        self._product_ids = None
        self._ingredient_ids = None
        self._inventory_map = TrackedMap(
            "inventory_map",
            default=0,
            encoder=int,
            decoder=int,
            missing_is_change=True,
        )
        self.structures = structures or []

    @property
    def sheets(self):
        if self._sheets is None:
            known_sheets = [
                "Meta",
                "Facility",
                "Products",
                "Recipes",
                "Market Transactions",
                "Transaction Import",
                "Inventory Import",
                "Job History",
                "Job Import",
            ]
            self._sheets = {
                k.lower().replace(" ", ""): self.spreadsheet.worksheet(k)
                for k in known_sheets
            }

        return self._sheets

    def translators(self, name):
        return self._translators.get(name.lower(), self._default_translators)

    def dictmap_list_to_list(self, dic, lst, default=None):
        return [dic.get(x, default) for x in lst]

    def threadmap_list_to_list(self, func, lst, max_workers=4):
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            return list(exe.map(func, lst))

    def sheet_threadmap(
        self,
        func,
        inp,
        xinp="",
        xout="col",
        max_workers=4,
    ):
        (inp_read, _) = self.translators(xinp)
        (_, out_write) = self.translators(xout)
        xs = inp_read(inp)
        ys = self.threadmap_list_to_list(func, xs, max_workers=max_workers)
        return out_write(ys)

    def sheet_dictmap(
        self,
        dic,
        inp,
        xinp="",
        xout="col",
        default=None,
    ):
        (inp_read, _) = self.translators(xinp)
        (_, out_write) = self.translators(xout)
        xs = inp_read(inp)
        ys = self.dictmap_list_to_list(dic, xs, default=default)
        return out_write(ys)

    @property
    def product_ids(self):
        return self.get_product_ids()

    def get_product_ids(self):
        return [
            int(x) if x else None
            for x in sh.get_col_range(
                self.sheets["products"].get_values("ProductIDs")
            )
        ]

    def update_product_ids(self):
        self._product_ids = None
        return self.product_ids

    @property
    def ingredient_ids(self):
        if self._ingredient_ids is None:
            self._ingredient_ids = self.get_ingredient_ids()
        return self._ingredient_ids

    def get_ingredient_ids(self):
        return [
            int(x) if x else None
            for x in sh.get_col_range(
                self.sheets["ingredients"].get_values("IngredientIDs")
            )
        ]

    def update_ingredient_ids(self):
        self._ingredient_ids = None
        return self._ingredient_ids

    def apply_product_dict(self, dic, cell_range, default=0):
        return self.sheets["products"].update(
            range_name=cell_range,
            values=self.sheet_dictmap(dic, self.product_ids, default=default),
        )

    def apply_ingredient_dict(self, dic, cell_range, default=0):
        return self.sheets["ingredients"].update(
            range_name=cell_range,
            values=self.sheet_dictmap(
                dic,
                self.ingredient_ids,
                default=default,
            ),
        )

    def update_stock(self):
        tots = self.ua.aggregate_on_field(
            "quantity",
            (
                asset for asset in self.ua.assets()
                if asset["location_id"] == self.station.id
            ),
        )
        self.apply_product_dict(tots, "ProductStock")
        self.apply_ingredient_dict(tots, "IngredientStock")

    def update_stock_anywhere(self):
        tots = self.ua.aggregate_on_field("quantity", self.ua.assets())
        self.apply_product_dict(tots, "ProductStock")
        self.apply_ingredient_dict(tots, "IngredientStock")

    def update_orders(self):
        orders = self.ua.orders()
        order_volume = self.ua.aggregate_on_field("volume_remain", orders)
        self.apply_product_dict(order_volume, "ProductOrderVolume")

        min_sell = {}
        for x in orders:
            type_id = x["type_id"]
            min_sell[type_id] = (
                min(min_sell[type_id], x["price"]) if type_id in min_sell
                else x["price"]
            )

        self.apply_product_dict(min_sell, "MinOrderPrice")

    def update_jobs(self):
        craft_volume = self.ua.aggregate_on_field(
            "runs",
            self.ua.jobs(),
            type_field="product_type_id",
        )
        self.apply_product_dict(craft_volume, "ProductCraftVolume")

    def update_base(self):
        mp = self.industry.market_prices()
        self.apply_ingredient_dict(mp["adjusted"], "IngredientBaseCost")
        self.apply_product_dict(mp["adjusted"], "ProductBaseCost")

    def update_product_prices(self, max_workers=6):

        product_ids = self.product_ids

        by_id = self._fetch_orders_by_id(product_ids, max_workers=max_workers)

        metrics = {
            x: self._market_metrics_from_orders(by_id, x)
            for x in product_ids
        }
        result = {}
        for x in metrics:
            for col in metrics[x]:
                if col not in result:
                    result[col] = {}
                result[col][x] = metrics[x][col]

        for k in result:
            self.apply_product_dict(
                result[k],
                f"Product {k}".replace(" ", ""),
            )

    def update_ingredient_prices(self, max_workers=6):

        ingredient_ids = self.ingredient_ids

        by_id = self._fetch_orders_by_id(
            ingredient_ids,
            max_workers=max_workers,
        )

        metrics = {
            ing_id: self._market_metrics_from_orders(by_id, ing_id)
            for ing_id in ingredient_ids
        }
        result = {}
        for ing_id in metrics:
            for col in metrics[ing_id]:
                if col not in result:
                    result[col] = {}
                result[col][ing_id] = metrics[ing_id][col]

        for k in result:
            self.apply_ingredient_dict(
                result[k],
                f"Ingredient {k}".replace(" ", ""),
            )

    def _fetch_orders_by_id(self, ids, max_workers=6):
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            entity_orders = list(
                exe.map(
                    lambda x: list(
                        self.order_fetcher.get_for_regions(
                            self.entity.strict.from_id(x),
                            REGIONS,
                        ),
                    ),
                    ids,
                )
            )

        return dict(zip(ids, entity_orders))

    def _fetch_orders_by_name(self, names, max_workers=6):
        with ThreadPoolExecutor(max_workers=max_workers) as exe:
            entity_orders = list(
                exe.map(
                    lambda x: list(
                        self.order_fetcher.get_for_regions(
                            self.entity.strict.from_name(x),
                            REGIONS,
                        ),
                    ),
                    names,
                )
            )

        entity_orders_from_structures = [
            list(
                itertools.chain.from_iterable(
                    self.order_fetcher.get_for_structure(
                        self.entity.strict.from_name(name),
                        struct,
                    )
                    for struct in self.structures
                )
            )
            for name in names
        ]

        zone_by_name = dict(zip(names, entity_orders))
        struct_by_name = dict(zip(names, entity_orders_from_structures))

        all_orders = {
            name: zone_by_name.get(name, []) + struct_by_name.get(name, [])
            for name in names
        }

        return all_orders

    def _market_metrics_from_orders(self, lookup, x):

        try:
            jita_sell_p1 = p1(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        jita_44,
                        EveMarketMetrics.filter_sell(lookup[x]),
                    ),
                ),
            )
        except ValueError:
            jita_sell_p1 = None

        try:
            jita_sell_p20 = p20(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        jita_44,
                        EveMarketMetrics.filter_sell(lookup[x]),
                    ),
                ),
            )
        except ValueError:
            jita_sell_p20 = None

        try:
            jita_buy_max = maximum(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        jita_44,
                        EveMarketMetrics.filter_buy(lookup[x]),
                    ),
                ),
            )
        except ValueError:
            jita_buy_max = None

        try:
            zone_sell_p1 = p1(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_sell(lookup[x]),
                ),
            )
        except ValueError:
            zone_sell_p1 = None

        try:
            zone_sell_p20 = p20(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_sell(lookup[x]),
                ),
            )
        except ValueError:
            zone_sell_p20 = None

        try:
            zone_buy_max = maximum(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_buy(lookup[x]),
                ),
            )
        except ValueError:
            zone_buy_max = None

        # try:
        #     k7_sell_p1 = p1(
        #         EveMarketMetrics.as_series(
        #             EveMarketMetrics.filter_location(
        #                 k7["entity"],
        #                 EveMarketMetrics.filter_sell(lookup[x]),
        #             ),
        #         ),
        #     )
        # except ValueError:
        #     k7_sell_p1 = None

        # try:
        #     k7_sell_p20 = p20(
        #         EveMarketMetrics.as_series(
        #             EveMarketMetrics.filter_location(
        #                 k7["entity"],
        #                 EveMarketMetrics.filter_sell(lookup[x]),
        #             ),
        #         ),
        #     )
        # except ValueError:
        #     k7_sell_p20 = None

        # try:
        #     k7_buy_max = maximum(
        #         EveMarketMetrics.as_series(
        #             EveMarketMetrics.filter_location(
        #                 k7["entity"],
        #                 EveMarketMetrics.filter_buy(lookup[x]),
        #             ),
        #         ),
        #     )
        # except ValueError:
        #     k7_buy_max = None

        # try:
        #     drac_sell_p1 = p1(
        #         EveMarketMetrics.as_series(
        #             EveMarketMetrics.filter_location(
        #                 drac["entity"],
        #                 EveMarketMetrics.filter_sell(lookup[x]),
        #             ),
        #         ),
        #     )
        # except ValueError:
        #     drac_sell_p1 = None

        # try:
        #     drac_sell_p20 = p20(
        #         EveMarketMetrics.as_series(
        #             EveMarketMetrics.filter_location(
        #                 drac["entity"],
        #                 EveMarketMetrics.filter_sell(lookup[x]),
        #             ),
        #         ),
        #     )
        # except ValueError:
        #     drac_sell_p20 = None

        # try:
        #     drac_buy_max = maximum(
        #         EveMarketMetrics.as_series(
        #             EveMarketMetrics.filter_location(
        #                 drac["entity"],
        #                 EveMarketMetrics.filter_buy(lookup[x]),
        #             ),
        #         ),
        #     )
        # except ValueError:
        #     drac_buy_max = None

        return {
            "Jita Sell p1": jita_sell_p1,
            "Jita Sell p20": jita_sell_p20,
            "Jita Buy Max": jita_buy_max,
            "Zone Sell p1": zone_sell_p1,
            "Zone Sell p20": zone_sell_p20,
            "Zone Buy Max": zone_buy_max,
            # "K7 Sell p1": k7_sell_p1,
            # "K7 Sell p20": k7_sell_p20,
            # "K7 Buy Max": k7_buy_max,
            # "Drac Sell p1": drac_sell_p1,
            # "Drac Sell p20": drac_sell_p20,
            # "Drac Buy Max": drac_buy_max,
        }

    def update_product_ids_from_names(self):
        names = sh.get_col_range(
            self.sheets["products"].get_values("A3:A500")
        )
        ids = self.sheet_threadmap(
            lambda n: entity.from_name(n).id,
            names,
        )
        self.sheets["products"].update(
            range_name="B3:B500",
            values=ids,
        )

    def update_recipes(self):
        ings = set([])
        rows = []

        product_names = self._names_from_sheet("Products", "Item", 2)
        entities = self.entity.from_name_seq(product_names)

        for outp in entities:
            ing_triples = blueprints.ingredients(outp).triples()
            ings = ings.union([entity for (_, _, entity) in ing_triples])
            ing_dict = {
                entity.id: quantity for (_, quantity, entity) in ing_triples
            }
            rows.append({"Product": outp.name, "ID": outp.id, **ing_dict})

        all_ings = list(ings)
        result = []
        result.append(["Product / Ingredient"] + [x.name for x in all_ings])
        for row in rows:
            result.append([row["Product"]] + [row.get(x.id, 0) for x in all_ings])
        self.sheets["recipes"].update(range_name="A8", values=result)

    def import_transactions(self):
        xaction_sheet = self.sheets["markettransactions"]
        import_sheet = self.sheets["transactionimport"]
        xaction_records = [
            record for record in
            xaction_sheet.get_all_records()
            if record.get("Date")
        ]
        import_records = sorted(
            import_sheet.get_all_records(),
            key=lambda x: x["Date"],
        )
        # Plus 1 because the header is in the sheet but not counted in the
        # record list
        last_row_index = len(xaction_records) + 1

        # Find the last 2 transactions in the transaction list in the import
        # list
        relevant = [
            "Date",
            "Amount",
            "Item",
            "Unit Price (str)",
            "Total Received",
            "Other Party",
            "Location",
        ]
        last_xaction = xaction_records[-1]
        last_xaction_fields = get(relevant, last_xaction)
        penult_xaction = xaction_records[-2]
        penult_xaction_fields = get(relevant, penult_xaction)
        import_record_seq = iter(import_records)
        previous = get(relevant, next(import_record_seq))
        for (i, current) in enumerate(import_record_seq):
            if (
                get(relevant, current) == last_xaction_fields
                and previous == penult_xaction_fields
            ):
                new_import_records = import_records[i+2:]
                break
            else:
                previous = get(relevant, current)
        else:
            new_import_records = import_records[:]

        new_import_grid = [
            [record[field] for field in relevant]
            for record in new_import_records
        ]
        start_row = last_row_index + 1
        end_row = start_row + len(new_import_grid)
        xaction_sheet.update(f"A{start_row}:G{end_row}", new_import_grid)

    def import_jobs(self):
        jh_sheet = self.sheets["jobhistory"]
        import_sheet = self.sheets["jobimport"]
        jh_records = [
            record for record in
            jh_sheet.get_all_records()
            if record.get("Install date")
        ]
        import_records = sorted(
            import_sheet.get_all_records(),
            key=lambda x: x["Install date"],
        )
        # Plus 1 because the header is in the sheet but not counted in the
        # record list
        last_row_index = len(jh_records) + 1

        # Find the last 2 records in the history in the import records
        relevant = [
            "Status",
            "Runs",
            "Activity",
            "Blueprint Name",
            "Jumps",
            "Security",
            "Facility",
            "Install date",
            "End date",
        ]
        last_jh = jh_records[-1]
        last_jh_fields = get(relevant, last_jh)
        penult_jh = jh_records[-2]
        penult_jh_fields = get(relevant, penult_jh)
        import_record_seq = iter(import_records)
        previous = get(relevant, next(import_record_seq))
        for (i, current) in enumerate(import_record_seq):
            if (
                get(relevant, current) == last_jh_fields
                and previous == penult_jh_fields
            ):
                new_import_records = import_records[i+2:]
                break
            else:
                previous = get(relevant, current)
        else:
            new_import_records = import_records[:]

        new_import_grid = [
            [record[field] for field in relevant]
            for record in new_import_records
        ]
        start_row = last_row_index + 1
        end_row = start_row + len(new_import_grid)
        jh_sheet.update(f"A{start_row}:I{end_row}", new_import_grid)

    def import_inventory(self):
        inv_import = self.sheets["inventoryimport"]
        data = {
            self.entity.from_name(x["Name"]).id: x.get("Amount") or 1
            for x in inv_import.get_all_records()
        }
        self._inventory_map.record(data)

    def update_stock_from_import(self, when=None):
        inv = {int(k): v for (k, v) in self._inventory_map.value(when).items()}
        self.apply_product_dict(inv, "ProductStock")
        self.apply_ingredient_dict(inv, "IngredientStock")

    def transactions_from_sheet(self):
        return sorted(
            self.sheets["markettransactions"].get_all_records(),
            key=lambda x: x["Date"],
        )

    def manufacture_history_from_sheet(self):
        manufacture_records = [
            record for record in self.sheets["jobhistory"].get_all_records()
            if (
                record["Status"] == "Succeeded"
                and record["Activity"] == "Manufacturing"
            )
        ]
        return sorted(manufacture_records, key=lambda x: x["Install date"])

    def update_sab(self):
        sab1 = sab(
            self.stock_source_sink_from_sheets(),
            # TODO: Should we pull this from the sheet?
            market_prices=self.industry.market_prices(),
        )
        self.apply_ingredient_dict(
            {k.id: v for (k, v) in sab1.items()},
            "G3:G100",
        )

    def stock_source_sink_from_sheets(self):
        xactions = self.transactions_from_sheet()
        job_hist = self.manufacture_history_from_sheet()
        jobs = sorted(
            [
                {
                    **record,
                    "Install date": record["start_date"].replace("T", " ")[:-4],
                }
                for record in self.ua.jobs()
            ],
            key=lambda x: x["Install date"],
        )

        interleaved = interleave_sorted(
            xactions, job_hist, jobs,
            keys=[
                lambda xact: xact["Date"],
                lambda jh: jh["Install date"],
                lambda j: j["Install date"],
            ],
        )

        for entry in interleaved:
            # Market Transactions
            if "Other Party" in entry:
                item = self.entity.from_name(entry["Item"]).entity
                is_buy = entry["Total Received"].startswith("-")
                amount = int(entry["Amount"])
                price = float(entry["Unit Price (str)"].replace(",", "")[:-4])

                if is_buy:
                    yield ("buy", item, amount, price)
                else:
                    yield ("sell", item, amount, price)

            # Historical job
            elif "Status" in entry and entry["Status"] == "Succeeded":
                end = len(" Blueprint")
                item = self.entity.from_name(entry["Blueprint Name"][:-end]).entity
                amount = int(entry["Runs"])
                ingredients = amount * self.blueprints.ingredients(item)
                yield ("craft", item, amount, ingredients)

            # Current job
            elif "status" in entry:
                if not (
                    entry["activity_id"] == 1 and entry["status"] == "active"
                ):
                    continue
                # Otherwise
                item = self.entity.from_id(entry["product_type_id"]).entity
                amount = entry["runs"]
                ingredients = amount * self.blueprints.ingredients(item)
                yield ("craft", item, amount, ingredients)

            # ???
            else:
                print(f"Wtf is this: {entry}")

    def _names_from_sheet(self, sheet_name, name_field, header_row):
        return sh.records_to_columns(
            sh.read_records(
                self.spreadsheet.worksheet(sheet_name),
                header_row=header_row,
                skip_when_field_empty=[name_field],
            )
        )[name_field]

    def update_invention(self):
        print("Collecting tech 2 BPCs...")
        product_names = self._names_from_sheet("Products", "Item", 2)
        bpc_names = [
            name for name in product_names if name.strip().endswith("II")
        ]
        invention_records = [
            {
                "Item": name,
                **self.blueprints.invention(self.entity.from_name(name)),
            }
            for name in bpc_names
        ]
        sh.insert_records(
            self.spreadsheet.worksheet("InventionImport"),
            invention_records,
            header_row=1,
            field_translation={
                "Science 1": "science1",
                "Datacore Multiplier 1": "datacore_mul1",
                "Science 2": "science2",
                "Datacore Multiplier 2": "datacore_mul2",
                "Encryption": "encryption",
                "Base Success Pct": "base_success_pct",
                "Runs Per Success": "runs_per",
            },
        )
        print("Collecting sciences...")
        sciences1 = self._names_from_sheet("InventionImport", "Science 1", 1)
        sciences2 = self._names_from_sheet("InventionImport", "Science 2", 1)
        encryption = self._names_from_sheet("InventionImport", "Encryption", 1)
        skills = list(unique(itertools.chain(sciences1, sciences2, encryption)))
        print("Updating skill list (skill level update still manual)...")
        self.spreadsheet.worksheet("Science").update(
            range_name="A2:A",
            values=[[skill] for skill in skills],
        )

    def update_prices(self):
        print("Collecting prices to check...")
        batch_names = self._names_from_sheet("Batches", "Item", 3)
        product_names = self._names_from_sheet("Products", "Item", 2)
        # Ingredients are taken from a row in the recipes sheet, so they're a
        # little weird
        ingredient_names = self.spreadsheet.worksheet("Recipes").get_values("B8:8")[0]
        science_names = self._names_from_sheet("Science", "Science", 1)
        datacore_names = [
            f"Datacore - {sci}" for sci in science_names
            if "Encryption Methods" not in sci
        ]
        names = list(
            unique(
                itertools.chain(
                    batch_names,
                    product_names,
                    ingredient_names,
                    datacore_names,
                )
            )
        )
        print(f"Found {len(names)} items to check.")
        mp = self.industry.market_prices()
        entities = self.entity.strict.from_name_seq(names)
        orders = self._fetch_orders_by_name(names, max_workers=6)
        metrics = [
            {
                "Item": entity.name,
                **self._market_metrics_from_orders(orders, entity.name),
                "Base Cost": mp["adjusted"][entity.id],
            }
            for entity in entities
        ]
        sh.insert_records(
            self.spreadsheet.worksheet("Prices"),
            metrics,
            header_row=1,
        )

    def update(self):
        self.order_fetcher.authed_requester.token.get()
        print("Updating recipes...")
        self.update_recipes()
        print("Updating invention...")
        self.update_invention()
        print("Updating prices...")
        self.update_prices()

    def update_sheet_stuff(self):
        print("Importing transactions...")
        self.import_transactions()
        print("Importing jobs...")
        self.import_jobs()
        print("Importing inventory...")
        self.import_inventory()
        print("Updating SAB...")
        self.update_sab()


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


def interleave_sorted(*seqs, keys=lambda x: x):
    _seqs = [iter(s) for s in seqs]
    if not isinstance(keys, (tuple, list)):
        keys = [keys]*len(_seqs)
    
    g = [next(s, None) for s in _seqs]

    while any(gg is not None for gg in g):
        x = [
            (i, k(s), s) for (i, (s, k)) in enumerate(zip(g, keys))
            if s is not None
        ]
        (pos, _, value) = min(x, key=lambda iks: iks[1])
        yield value
        g = [
            next(s, None) if i == pos else gg
            for (i, (gg, s)) in enumerate(zip(g, _seqs))
        ]


def sab(
    updates,
    market_prices: dict,
    # TODO: Pull this from the sheet I guess?
    install_factor=0.1,
):
    amounts = {}
    sabs = {}

    for record in updates:

        match record:

            case ("stock_source", item, amount):
                amounts[item] = max(0, amounts.get(item, 0) + amount)

            case ("stock_sink", item, amount):
                amounts[item] = max(0, amounts.get(item, 0) - amount)

            case ("sab_update", item, amount, price):
                if sabs.get(item, 0) == 0:
                    sabs[item] = price
                else:
                    current_amt = amounts.get(item, 0)
                    sab = sabs[item]
                    sabs[item] = (
                        (current_amt * sab + amount * price)
                        / (current_amt + amount)
                    )

            case ("buy", item, amount, price):
                current_amt = amounts.get(item, 0)
                sab = sabs.get(item, 0)
                sabs[item] = (
                    (current_amt * sab + amount * price)
                    / (current_amt + amount)
                )
                amounts[item] = amounts.get(item, 0) + amount

            case ("sell", item, amount, price):
                current_amt = amounts.get(item, 0)
                sab = sabs.get(item, 0)
                sabs[item] = (
                    (current_amt * sab + amount * price)
                    / (current_amt + amount)
                )
                # We might have had some extra stock of the ingredient that we
                # didn't gain from our transaction history.  Don't reduce the
                # amount below zero!
                amounts[item] = max(amounts.get(item, 0) - amount, 0)

            # ingredients already factors in the amount of output produced,
            # i.e. ingredients = amount * per_unit_ingredients
            case ("craft", item, amount, ingredients):
                mp = market_prices
                install_cost = (
                    install_factor * amount * mp["adjusted"][item.id]
                )
                ingredient_cost = sum(
                    amt * mp["adjusted"][it.id]
                    for (_, amt, it) in ingredients.triples()
                )
                cost = (install_cost + ingredient_cost) / amount
                current_amt = amounts.get(item, 0)
                sab = sabs.get(item, 0)
                sabs[item] = (
                    (current_amt * sab + amount * cost)
                    / (current_amt + amount)
                )
                amounts[item] = amounts.get(item, 0) + amount
                for (_, amt, it) in ingredients.triples():
                    # We might have had some extra stock of the ingredient that
                    # we didn't gain from our transaction history.  Don't
                    # reduce the amount below zero!
                    amounts[it] = max(amounts.get(it, 0) - amt, 0)

    return sabs


si = SheetInterface(
    spreadsheet,
    ua,
    entity,
    order_fetcher,
    industry,
    blueprints,
    station=jita_44,
    structures=[],
)
