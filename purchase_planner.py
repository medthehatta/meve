import re
import os
from dataclasses import dataclass
import json
import pickle
from pprint import pprint
import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import networkx as nx
from cytoolz import get_in
from cytoolz import groupby
from cytoolz import valmap
from bs4 import BeautifulSoup

from formal_vector import FormalVector
from hxxp import Requester
from hxxp import DefaultHandlers
from authentication import EmptyToken
import authentication as auth

from requests.exceptions import JSONDecodeError


_json = DefaultHandlers.raise_or_return_json


class DOES_NOT_EXIST:

    def __repr__(self):
        return "DOES_NOT_EXIST"


DNE = DOES_NOT_EXIST()


def slurp_json(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    else:
        return DNE


def dump_json(data, path):
    with open(path, "w") as f:
        return json.dump(data, f)


def slurp_pickle(path):
    if os.path.exists(path):
        with open(path, "rb") as f:
            return pickle.load(f)
    else:
        return DNE


def dump_pickle(data, path):
    with open(path, "wb") as f:
        return pickle.dump(data, f)






client_id = "2ca75dd163354b0794cca4726d631df4"
client_secret = "eHwPGFnA99aGu784pJBqv3U7mi9t6IfaNbkUmoKN"

tok = auth.EveOnlineFlow(
    "https://login.eveonline.com/v2/oauth/token",
    client_id=client_id,
    client_secret=client_secret,
    scopes=[
        "esi-wallet.read_character_wallet.v1",
        "esi-wallet.read_corporation_wallet.v1",
        "esi-assets.read_assets.v1",
        "esi-markets.structure_markets.v1",
        "esi-markets.read_character_orders.v1",
        "esi-wallet.read_corporation_wallets.v1",
        "esi-assets.read_corporation_assets.v1",
        "esi-markets.read_corporation_orders.v1",
    ],
    code_fetcher=auth.get_code_http(8080),
)
if os.path.exists("token.pkl"):
    token_data = slurp_pickle("token.pkl")
    tok.tokens = token_data["tokens"]
    tok.expire_time = token_data["expire_time"]
    tok.refresh_expire_time = token_data["refresh_expire_time"]
#tok.get()
dump_pickle(
    {
        "tokens": tok.tokens,
        "expire_time": tok.expire_time,
        "refresh_expire_time": tok.refresh_expire_time,
    },
    "token.pkl",
)

r0 = Requester("https://esi.evetech.net/latest/", EmptyToken())
r = Requester("https://esi.evetech.net/latest/", tok)
zk = Requester("https://zkillboard.com", EmptyToken())


from universe import UserAssets
from universe import UniverseLookup
from universe import ItemFactory
from universe import BlueprintLookup
from universe import Ingredients
from purchase_tour import orders_by_location
from purchase_tour import orders_by_item
from purchase_tour import orders_in_regions
from cli import DEFAULT_REGION_NAMES
from delayed import Delayed


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
blueprints = BlueprintLookup(items)
#ua = UserAssets(r, "Mola Pavonis")
ingredients_parser = lambda s: Ingredients.parse_with_item_lookup(s, items=items)


DEFAULT_REGION_IDS = [universe.from_name(x).id for x in DEFAULT_REGION_NAMES]
SOME_ITEM_NAMES = [
    "Small Auxiliary Thrusters I",
    "Small Cargohold Optimization I",
    "Small Low Friction Nozzle Joints I",
    "Small Polycarbon Engine Housing I",
    "Small Signal Focusing Kit I",
]
SOME_ITEM_IDS = [
    universe.from_name(x).id
    for x in [
        y.strip() for y in SOME_ITEM_NAMES
        if y.strip()
    ]
]
DEFAULT_LOCATION_NAMES = [
    "Stacmon V - Moon 9 - Federation Navy Assembly Plant",
    "Clellinon VI - Moon 11 - Center for Advanced Studies School",
    "Villore VII - Moon 8 - Quafe Company Factory",
    "Dodixie IX - Moon 20 - Federation Navy Assembly Plant",
    "Alentene VI - Moon 6 - Roden Shipyards Warehouse",
]
DEFAULT_LOCATION_IDS = [
    universe.from_name(x).id for x in DEFAULT_LOCATION_NAMES
]


class CraftingPrices:

    def __init__(self, blueprints, user_assets):
        self.blueprints = blueprints
        self.user_assets = user_assets

    def smart_avg_crafting_price(self, item_id):
        return sum(
            amt * self.user_assets.smart_avg_buy(ing_id)
            for (amt, ing_id) in self.blueprints.ingredients(
                entity_id=item_id,
            )
        )


# TODO: Add caching; generally reduce reliance on making API calls
def minimum_crafted_sell_price(blueprints, item_id, multiplier=1.2):
    return multiplier*sum(
        amt * ua.smart_avg_buy(entity_id)
        for (amt, entity_id) in blueprints.ingredients(entity_id=item_id)
    )


class MarketMetrics:

    @classmethod
    def from_regions(cls, requester, region_ids, item_ids):
        return cls(orders_in_regions(requester, region_ids, item_ids))

    def __init__(self, orders):
        self.by_item = orders_by_item(orders)

    def sell_price_series(self, item_id):
        return [
            x["price"] for x in itertools.chain.from_iterable(
                self.by_item.get(item_id, {}).values()
            )
            if not x["is_buy_order"]
        ]

    def buy_price_series(self, item_id):
        return [
            x["price"] for x in itertools.chain.from_iterable(
                self.by_item.get(item_id, {}).values()
            )
            if x["is_buy_order"]
        ]

    def sell_quantity_series(self, item_id):
        return [
            x["volume_total"] for x in itertools.chain.from_iterable(
                self.by_item.get(item_id, {}).values()
            )
            if not x["is_buy_order"]
        ]

    def buy_quantity_series(self, item_id):
        return [
            x["volume_total"] for x in itertools.chain.from_iterable(
                self.by_item.get(item_id, {}).values()
            )
            if x["is_buy_order"]
        ]

    def avg_sell(self, item_id):
        series = self.sell_price_series(item_id)
        return sum(series) / len(series)

    def weighted_avg_sell(self, item_id):
        prices = self.sell_price_series(item_id)
        quantities = self.sell_quantity_series(item_id)
        total_cost = sum(x*y for (x, y) in zip(prices, quantities))
        total_purchased = sum(quantities)
        return total_cost / total_purchased

    def avg_buy(self, item_id):
        series = self.buy_price_series(item_id)
        return sum(series) / len(series)

    def weighted_avg_buy(self, item_id):
        prices = self.buy_price_series(item_id)
        quantities = self.buy_quantity_series(item_id)
        total_cost = sum(x*y for (x, y) in zip(prices, quantities))
        total_purchased = sum(quantities)
        return total_cost / total_purchased

    def local_sell_price_series(self, item_id, location_id):
        return [
            x["price"]
            for x in self.by_item.get(item_id, {}).get(location_id, [])
            if not x["is_buy_order"]
        ]

    def local_sell_quantity_series(self, item_id, location_id):
        return [
            x["volume_total"]
            for x in self.by_item.get(item_id, {}).get(location_id, [])
            if not x["is_buy_order"]
        ]

    def local_buy_price_series(self, item_id, location_id):
        return [
            x["price"]
            for x in self.by_item.get(item_id, {}).get(location_id, [])
            if x["is_buy_order"]
        ]

    def local_buy_quantity_series(self, item_id, location_id):
        return [
            x["volume_total"]
            for x in self.by_item.get(item_id, {}).get(location_id, [])
            if x["is_buy_order"]
        ]

    def avg_local_sell(self, item_id, location_id):
        series = self.local_sell_price_series(item_id, location_id)
        return sum(series) / len(series)

    def weighted_avg_local_sell(self, item_id, location_id):
        prices = self.local_sell_price_series(item_id, location_id)
        quantities = self.local_sell_quantity_series(item_id, location_id)
        total_cost = sum(x*y for (x, y) in zip(prices, quantities))
        total_purchased = sum(quantities)
        return total_cost / total_purchased

    def avg_local_buy(self, item_id, location_id):
        series = self.local_buy_price_series(item_id, location_id)
        return sum(series) / len(series)

    def weighted_avg_local_buy(self, item_id, location_id):
        prices = self.local_buy_price_series(item_id, location_id)
        quantities = self.local_buy_quantity_series(item_id, location_id)
        total_cost = sum(x*y for (x, y) in zip(prices, quantities))
        total_purchased = sum(quantities)
        return total_cost / total_purchased

    def max_sell(self, item_id):
        return max(self.sell_price_series(item_id))

    def local_max_sell(self, item_id, location_id):
        return max(self.local_sell_price_series(item_id, location_id))

    def max_buy(self, item_id):
        return max(self.buy_price_series(item_id))

    def local_max_buy(self, item_id, location_id):
        return max(self.local_buy_price_series(item_id, location_id))

    def min_sell(self, item_id):
        return min(self.sell_price_series(item_id))

    def local_min_sell(self, item_id, location_id):
        return min(self.local_sell_price_series(item_id, location_id))

    def min_buy(self, item_id):
        return min(self.buy_price_series(item_id))

    def local_min_buy(self, item_id, location_id):
        return min(self.local_buy_price_series(item_id, location_id))


@dataclass
class MarketSeries:

    def _prices(self):
        raise NotImplementedError()

    def _quantities(self):
        raise NotImplementedError()

    @property
    def prices(self):
        if self._price_series is None:
            self._price_series = list(self._prices())
        return self._price_series

    @property
    def quantities(self):
        if self._quantity_series is None:
            self._quantity_series = list(self._quantities())
        return self._quantity_series


@dataclass
class LocalSellSeries(MarketSeries):

    item_id: int
    location_id: int

    def _prices(self):
        return [
            x["price"]
            for x in get_in(
                [self.item_id, self.location_id],
                self.by_item,
                default=[],
            )
            if not x["is_buy_order"]
        ]

    def _quantities(self):
        return [
            x["volume_total"]
            for x in get_in(
                [self.item_id, self.location_id],
                self.by_item,
                default=[],
            )
            if not x["is_buy_order"]
        ]


@dataclass
class SellSeries(MarketSeries):

    item_id: int

    def _prices(self):
        return [
            x["price"] for x in itertools.chain.from_iterable(
                self.by_item.get(self.item_id, {}).values()
            )
            if not x["is_buy_order"]
        ]

    def _quantities(self):
        return [
            x["volume_total"] for x in itertools.chain.from_iterable(
                self.by_item.get(self.item_id, {}).values()
            )
            if not x["is_buy_order"]
        ]


@dataclass
class LocalBuySeries(MarketSeries):

    item_id: int
    location_id: int

    def _prices(self):
        return [
            x["price"]
            for x in get_in(
                [self.item_id, self.location_id],
                self.by_item,
                default=[],
            )
            if x["is_buy_order"]
        ]

    def _quantities(self):
        return [
            x["volume_total"]
            for x in get_in(
                [self.item_id, self.location_id],
                self.by_item,
                default=[],
            )
            if x["is_buy_order"]
        ]


@dataclass
class BuySeries(MarketSeries):

    item_id: int

    def _prices(self):
        return [
            x["price"] for x in itertools.chain.from_iterable(
                self.by_item.get(self.item_id, {}).values()
            )
            if x["is_buy_order"]
        ]

    def _quantities(self):
        return [
            x["volume_total"] for x in itertools.chain.from_iterable(
                self.by_item.get(self.item_id, {}).values()
            )
            if x["is_buy_order"]
        ]


class MarketSeriesFactory:

    @classmethod
    def from_regions(cls, requester, region_ids, item_ids):
        return cls(orders_in_regions(requester, region_ids, item_ids))

    def __init__(self, orders):
        self.by_item = orders_by_item(orders)

    def sell_series(self, item_id):
        return SellSeries(self.by_item, item_id)

    def buy_series(self, item_id):
        return BuySeries(self.by_item, item_id)

    def local_sell_series(self, item_id, location_id):
        return LocalSellSeries(self.by_item, item_id, location_id)

    def local_buy_series(self, item_id, location_id):
        return LocalBuySeries(self.by_item, item_id, location_id)


@dataclass
class SeriesMetrics:

    series: MarketSeries

    @property
    def average(self):
        return sum(self.series.prices) / len(self.series.prices)

    @property
    def weighted_average(self):
        total_cost = sum(
            x*y for (x, y) in zip(self.series.prices, self.series.quantities)
        )
        total_purchased = sum(self.series.quantities)
        return total_cost / total_purchased

    @property
    def maximum(self):
        return max(self.series.prices)

    @property
    def minimum(self):
        return min(self.series.prices)

    def percentile(self, pct):
        seq = self.series.prices
        ordered = sorted(seq)
        N = len(ordered)
        k_d = (pct/100) * N
        k = int(k_d)
        d = k_d - k
        if k == 0:
            return ordered[0]
        elif k >= N:
            return ordered[-1]
        else:
            return ordered[k] + d*(ordered[k+1] - ordered[k])


def choose_sell_operation(mkt_series_factory, item_id, location_id):
    min_craft = None  # TODO
    sell = SeriesMetrics(mkt_series_factory.sell_series(item_id))
    local_sell = SeriesMetrics(
        mkt_series_factory.local_sell_series(item_id, location_id)
    )
    buy = SeriesMetrics(mkt_series_factory.buy_series(item_id))
    local_buy = SeriesMetrics(
        mkt_series_factory.local_buy_series(item_id, location_id)
    )
    return {
        "minimum": max(local_sell.minimum - 100, min_craft),
        "normal": sell.average,
        "maximum": 2*sell.maximum,
    }


item_listing = """
Small Auxiliary Thrusters I	10
Small Cargohold Optimization I	10
Small Low Friction Nozzle Joints I	10
Small Polycarbon Engine Housing I	10
Small Signal Focusing Kit I	10
"""


from collections import defaultdict


imicus_fit = """
imicus
3 nanofiber internal structure i
5mn y-t8 compact microwarpdrive
data analyzer i
relic analyzer i
cargo scanner i
prototype cloaking device i
core probe launcher i
salvager i
2 small gravity capacitor upgrade i
"""


imicus_ingredients = ingredients_parser(
    " + ".join(line for line in imicus_fit.splitlines() if line.strip())
)


def style(element):
    s = element.attrs.get("style", "")
    items = [x.strip() for x in s.split(";")]
    kv = [y for y in [x.split(":", 1) for x in items] if len(y) == 2]
    return {k: v.strip() for (k, v) in kv}


def kills_soup(character_name):
    character_id = universe.from_name(character_name).id
    return BeautifulSoup(zk.request("GET", f"/character/{character_id}/").text)


def pad_lst_to_len(length, value=None):

    def _pad_lst_to_len(lst):
        current = len(lst)
        padding = [value]*(length - current)
        return lst + padding

    return _pad_lst_to_len
    

def killdata(character_name):
    pad = pad_lst_to_len(6, "")

    soup = kills_soup(character_name)

    danger_percents = [x.text for x in soup.find_all("div", attrs={"class": "progress-bar-danger"})]
    snuggly_percents = [
        x.text for x in soup.find_all("div", attrs={"class": "progress-bar"})
        if "progress-bar-danger" not in x.attrs.get("class", [])
    ]

    danger_numbered = [
        int(x.strip().strip("%")) if x.strip() else 0 for x in danger_percents
    ]
    snuggly_numbered = [
        int(x.strip().strip("%")) if x.strip() else 0 for x in snuggly_percents
    ]

    percents = list(zip(pad(danger_numbered), pad(snuggly_numbered)))

    dangerous = [
        danger if danger else (100 - snuggly) if snuggly else 0
        for (danger, snuggly) in percents
    ]

    match dangerous:

        case [da, _, None, *_]:
            (danger_recent, danger_all) = (0, da)

        case [da, _, dr, *_]:
            (danger_recent, danger_all) = (dr, da)

        case _:
            (danger_recent, danger_all) = (0, 0)

    return (danger_recent, danger_all)


class Mapper:

    def map(self, func, seq):
        for s in seq:
            yield func(s)


@contextmanager
def FlatExecutor(*args, **kwargs):
    yield Mapper()


def killdata_from_stream(stream):
    keys = [x for x in (line.strip() for line in stream) if x]
    with ThreadPoolExecutor(max_workers=6) as exe:
        values = list(exe.map(killdata, keys))

    return dict(zip(keys, values))


def ranked_threats(data):
    return sorted(((v, k) for (k, v) in data.items()), reverse=True)




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


DEFAULT_REGION_NAMES = [
    "Verge Vendor",
    "Placid",
    "Essence",
    "Sinq Laison",
]


def sell_orders(desired):
    # Better name for the variable
    region_names = DEFAULT_REGION_NAMES

    order_type = "sell"

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

        yield entry1


def postprocess(required, orders):
    satisfactory = [
        order for order in orders
        if order["volume"] >= required[order["item"].lower()]
    ]

    by_item = groupby(lambda x: x["item"], satisfactory)

    best_by_location = {}

    for (item, item_entries) in by_item.items():
        for (location, location_entries) in groupby(lambda x: x["station"], item_entries).items():
            if item not in best_by_location:
                best_by_location[item] = {}
            best_by_location[item][location] = min(location_entries, key=lambda x: x["price"])

    sorted_by_location = {}

    for item in best_by_location:
        sorted_by_location[item] = sorted(best_by_location[item].values(), key=lambda y: y["price"])
        sorted_by_location[item] = [
            {"station": x["station"], "price": x["price"]}
            for x in sorted_by_location[item]
        ]

    return sorted_by_location


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "items",
        type=argparse.FileType("r"),
        nargs="?",
        default="-",
    )
    parsed = parser.parse_args()

    items = parsed.items

    desired = parse_recipe_lines(items)
    required = {item: count for (count, item) in desired}

    orders = sell_orders(desired)

    sorted_by_location = postprocess(required, orders)

    print(json.dumps(sorted_by_location))



if __name__ == "__main__":
    main()
