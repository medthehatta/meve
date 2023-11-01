from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import glob
import itertools
import datetime
from pprint import pprint
import pickle

from cytoolz import get

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

from tracked_map import TrackedMap

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

    @property
    def sheets(self):
        if self._sheets is None:
            known_sheets = [
                "Meta",
                "Facility",
                "Products",
                "Ingredients",
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

    def update_ingredient_base(self):
        mp = self.industry.market_prices()
        self.apply_ingredient_dict(mp["adjusted"], "IngredientBaseCost")

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
            x: self._market_metrics_from_orders(by_id, x)
            for x in ingredient_ids
        }
        result = {}
        for x in metrics:
            for col in metrics[x]:
                if col not in result:
                    result[col] = {}
                result[col][x] = metrics[x][col]

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

    def _market_metrics_from_orders(self, by_id, x):
        return {
            "Dodixie Sell p1": p1(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        dodixie_fed,
                        EveMarketMetrics.filter_sell(by_id[x]),
                    ),
                ),
            ),
            "Dodixie Sell p20": p20(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        dodixie_fed,
                        EveMarketMetrics.filter_sell(by_id[x]),
                    ),
                ),
            ),
            "Dodixie Buy p90": p90(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        dodixie_fed,
                        EveMarketMetrics.filter_buy(by_id[x]),
                    ),
                ),
            ),
            "Jita Sell p1": p1(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        jita_44,
                        EveMarketMetrics.filter_sell(by_id[x]),
                    ),
                ),
            ),
            "Jita Sell p20": p20(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        jita_44,
                        EveMarketMetrics.filter_sell(by_id[x]),
                    ),
                ),
            ),
            "Jita Buy p90": p90(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_location(
                        jita_44,
                        EveMarketMetrics.filter_buy(by_id[x]),
                    ),
                ),
            ),
            "Zone Sell p1": p1(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_sell(by_id[x]),
                ),
            ),
            "Zone Sell p20": p20(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_sell(by_id[x]),
                ),
            ),
            "Zone Buy p90": p90(
                EveMarketMetrics.as_series(
                    EveMarketMetrics.filter_buy(by_id[x]),
                ),
            ),
        }

    def update_recipes(self):
        ings = set([])
        rows = []

        entities = self.entity.from_id_seq(self.product_ids)

        for outp in entities:
            ing_triples = blueprints.ingredients(outp).triples()
            ings = ings.union([entity for (_, _, entity) in ing_triples])
            ing_dict = {
                entity.id: quantity for (_, quantity, entity) in ing_triples
            }
            rows.append({"Product": outp.name, "ID": outp.id, **ing_dict})

        all_ings = list(ings)
        result = []
        result.append(["", "Ingredient"] + [x.name for x in all_ings])
        result.append(["Product", "ID"] + [x.id for x in all_ings])
        for row in rows:
            result.append([row["Product"], row["ID"]] + [row.get(x.id, 0) for x in all_ings])
        self.sheets["recipes"].update(range_name="A2", values=result)

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
        sab1 = sab(self.stock_source_sink_from_sheets())
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
                    yield ("sab_update", item, amount, price)
                    yield ("stock_source", item, amount)
                else:
                    yield ("stock_sink", item, amount)

            # Historical job
            elif "Status" in entry and entry["Status"] == "Succeeded":
                end = len(" Blueprint")
                item = self.entity.from_name(entry["Blueprint Name"][:-end]).entity
                amount = int(entry["Runs"])
                yield ("stock_source", item, amount)
                inputs = amount * self.blueprints.ingredients(item)
                for (_, amt, it) in inputs.triples():
                    yield ("stock_sink", it, amt)

            # Current job
            elif "status" in entry:
                if not (
                    entry["activity_id"] == 1 and entry["status"] == "active"
                ):
                    continue
                # Otherwise 
                item = self.entity.from_id(entry["product_type_id"]).entity
                amount = entry["runs"]
                yield ("stock_source", item, amount)
                inputs = amount * self.blueprints.ingredients(item)
                for (_, amt, it) in inputs.triples():
                    yield ("stock_sink", it, amt)

            # ???
            else:
                print(f"Wtf is this: {entry}")


    def update(self):
        print("Updating recipes...")
        self.update_recipes()
        print("Updating stock...")
        self.update_stock()
        print("Updating orders...")
        self.update_orders()
        print("Updating jobs...")
        self.update_jobs()
        print("Updating ingredient base prices...")
        self.update_ingredient_base()
        print("Updating ingredient market prices...")
        # FIXME: Reset cache: somehow this is screwing me up even though the
        # eviction policy is clear
        self.order_fetcher._orders.clear()
        self.update_ingredient_prices()
        print("Updating product market prices...")
        self.update_product_prices()

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


def sab(updates):
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

    return sabs


si = SheetInterface(spreadsheet, ua, entity, order_fetcher, industry, blueprints, dodixie_fed)
