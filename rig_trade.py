import os
from dataclasses import dataclass
from typing import Optional
import json
import pickle
from pprint import pprint
import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import networkx as nx
from cytoolz import get_in
from cytoolz import keymap
from cytoolz import valmap
from cytoolz import curry
from bs4 import BeautifulSoup
import diskcache

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
    disk_path="token.pkl",
)

r0 = Requester("https://esi.evetech.net/latest/", EmptyToken())
r = Requester("https://esi.evetech.net/latest/", tok)
zk = Requester("https://zkillboard.com", EmptyToken())


from universe import UserAssets
from universe import UniverseLookup
from universe import ItemFactory
from universe import BlueprintLookup
from universe import Ingredients
from universe import EntityFactory
from universe import Industry
from purchase_tour import orders_by_location
from purchase_tour import orders_in_regions
from purchase_tour import orders_for_item
from purchase_tour import orders_for_item_at_location
from purchase_tour import orders_for_item_in_system
from cli import DEFAULT_REGION_NAMES
from delayed import Delayed


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
entity = EntityFactory(items, universe)
blueprints = BlueprintLookup(items, entity)
ua = UserAssets(r, "Mola Pavonis")
ingredients_parser = lambda s: Ingredients.parse_with_item_lookup(s, items=items)


DEFAULT_REGION_IDS = [entity(name=x).id for x in DEFAULT_REGION_NAMES]
SOME_ITEM_NAMES = [
    "Small Auxiliary Thrusters I",
    "Small Cargohold Optimization I",
    "Small Low Friction Nozzle Joints I",
    "Small Polycarbon Engine Housing I",
    "Small Signal Focusing Kit I",
]
SOME_ITEM_IDS = [
    entity(name=x).id
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
    entity(name=x).id for x in DEFAULT_LOCATION_NAMES
]


class CraftingPrices:

    def __init__(self, blueprints, user_assets):
        self.blueprints = blueprints
        self.user_assets = user_assets

    def smart_avg_crafting_price(self, entity):
        return sum(
            amt * self.user_assets.smart_avg_buy(ing_id)
            for (_, amt, ing_id) in self.blueprints.ingredients(entity).triples()
        )


def first(seq):
    return next((s for s in seq if s is not None), None)


# TODO: Add caching; generally reduce reliance on making API calls
def minimum_crafted_sell_price(ua, blueprints, entity, ent, multiplier=1.2):
    return multiplier*sum(
        amt * ua.smart_avg_buy(entity_id)
        for (_, amt, entity_id) in blueprints.ingredients(ent).triples()
    )


def minimum_sell_price_listing(blueprints, listing, multiplier=1.2):
    for (entry, _, _) in listing.triples():
        price = minimum_crafted_sell_price(blueprints, items.from_terms(entry).id, multiplier)
        print(f"{entry}\t{price}")


class WeightedSeries:

    @classmethod
    def from_record_sequence(cls, seq, value_key, weight_key=None):
        pairs = [
            (record.get(value_key), record.get(weight_key, 1))
            for record in seq
        ]
        values = [v for (v, _) in pairs]
        weights = [w for (_, w) in pairs]
        return cls(values, weights)

    def __init__(self, values, weights=None):
        self._values = values
        self._weights = weights

    @property
    def values(self):
        return self._values

    @property
    def weights(self):
        if self._weights is None:
            return [1]*len(self.values)
        else:
            return self._weights

    def __repr__(self):
        return f"<WeightedSeries (size: {len(self.values)})>"


class WeightedSeriesMetrics:

    @classmethod
    def average(cls, series):
        return sum(series.values) / len(series.values)

    @classmethod
    def weighted_average(cls, series):
        total_cost = sum(
            x*y for (x, y) in zip(series.values, series.weights)
        )
        total_purchased = sum(series.weights)
        return total_cost / total_purchased

    @classmethod
    def maximum(cls, series):
        return max(series.values)

    @classmethod
    def minimum(cls, series):
        return min(series.values)

    @classmethod
    @curry
    def percentile(cls, pct, series):
        seq = series.values
        ordered = sorted(seq)
        N = len(ordered)
        k_d = (pct/100) * N
        k = int(k_d)
        d = k_d - k
        if k == 0:
            return ordered[0]
        elif k >= N-1:
            return ordered[-1]
        else:
            return ordered[k] + d*(ordered[k+1] - ordered[k])

    @classmethod
    def total_weight(cls, series):
        return sum(series.weights)


class OrderFetcher:

    def __init__(
        self,
        universe,
        expire=60,
        disk_cache=None,
    ):
        self.universe = universe
        # FIXME: Ugh, but we can fix the DI later
        self.requester = self.universe.requester
        self.default_expire = expire
        if disk_cache:
            self._orders = diskcache.Cache(disk_cache)
        else:
            self._orders = diskcache.Cache()

    def get_for_station(self, entity, station, expire=None):
        expire = expire or self.default_expire
        region = universe.chain(
            station, "station", "system", "constellation", "region",
        )
        key = (entity.id, region.id)

        if key not in self._orders:
            seq = orders_for_item(self.requester, [region.id], entity.id)
            self._orders.set(key, list(seq), expire=expire)

        return self._orders[key]

    def get_for_regions(self, entity, regions, expire=None):
        expire = expire or self.default_expire
        for region in regions:
            key = (entity.id, region.id)

            if key not in self._orders:
                seq = orders_for_item(self.requester, [region.id], entity.id)
                self._orders.set(key, list(seq), expire=expire)

        keys = [(entity.id, region.id) for region in regions]
        return itertools.chain.from_iterable(self._orders[k] for k in keys)


class EveMarketMetrics:

    @classmethod
    def as_series(cls, orders):
        return WeightedSeries.from_record_sequence(
            orders,
            value_key="price",
            weight_key="volume_total",
        )

    @classmethod
    @curry
    def filter_location(cls, location, orders):
        return [
            x for x in orders if x["location_id"] == location.id
        ]

    @classmethod
    def filter_buy(cls, orders):
        return [x for x in orders if x["is_buy_order"]]

    @classmethod
    def filter_sell(cls, orders):
        return [x for x in orders if not x["is_buy_order"]]

    @classmethod
    def local_buy_series(cls, location, orders):
        return cls.as_series(
            cls.filter_location(location, cls.filter_buy(orders)),
        )

    @classmethod
    def local_sell_series(cls, location, orders):
        return cls.as_series(
            cls.filter_location(location, cls.filter_sell(orders)),
        )


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
    character_id = entity(name=character_name).id
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



def _truthy(seq):
    return [x for x in seq if x]




class MfgMarket:

    def __init__(
        self,
        industry,
        order_fetcher,
        mfg_station,
        sell_station=None,
        buy_station=None,
        broker_fee_percent=3,
        accounting_level=0,
    ):
        self.industry = industry
        self.order_fetcher = order_fetcher
        self.mfg_station = mfg_station
        self.buy_station = buy_station or sell_station or mfg_station
        self.sell_station = sell_station or buy_station or mfg_station
        self.broker_fee_percent = broker_fee_percent
        self.accounting_level = accounting_level

    def variant(
        self,
        industry=None,
        order_fetcher=None,
        sell_station=None,
        mfg_station=None,
        buy_station=None,
        broker_fee_percent=None,
        accounting_level=None,
    ):
        return type(self)(
            industry=industry or self.industry,
            order_fetcher=order_fetcher or self.order_fetcher,
            sell_station=sell_station or self.sell_station,
            mfg_station=mfg_station or self.mfg_station,
            buy_station=buy_station or self.buy_station,
            broker_fee_percent=broker_fee_percent or self.broker_fee_percent,
            accounting_level=accounting_level or self.accounting_level,
        )

    def manufacture_metrics(self, entity, alpha=False):
        return self.industry.installation_cost_verbose(
            item_entity=entity,
            facility_entity=self.mfg_station,
            alpha=alpha,
        )

    def ingredients_buy(self, ingredients):
        result = {
            e: EveMarketMetrics.local_sell_series(
                self.buy_station,
                self.order_fetcher.get_for_station(e, self.buy_station),
            )
            for (_, _, e) in ingredients.triples()
        }
        return result

    def total_cost_with_ingredient_prices(
        self,
        entity,
        ingredient_buy_metric=WeightedSeriesMetrics.percentile(20),
        item_sell_metric=WeightedSeriesMetrics.minimum,
        alpha=False,
        make_components=None,
    ):
        manufacture = self.industry.manufacture(
            entity,
            self.mfg_station,
            alpha=alpha,
            make_components=make_components,
        )
        ingredients = manufacture["raw_ingredients"]
        prices = valmap(
            ingredient_buy_metric,
            self.ingredients_buy(ingredients),
        )
        mat_prices = {
            entity: {
                "individual": prices[entity],
                "job": quantity * prices[entity],
            }
            for (_, quantity, entity) in ingredients.triples()
        }
        mat_prices["total"] = sum(entry["job"] for entry in mat_prices.values())
        sell = item_sell_metric(
            EveMarketMetrics.local_sell_series(
                self.sell_station,
                self.order_fetcher.get_for_station(entity, self.sell_station),
            )
        )
        sales_tax_rate = 8*(1 - 0.11/100 * self.accounting_level)/100
        sales_tax = sales_tax_rate * sell
        broker_fee = max(100, self.broker_fee_percent/100 * sell)
        return {
            "item": entity,
            "sell_station": self.sell_station,
            "buy_station": self.buy_station,
            "manufacture": manufacture,
            "materials": mat_prices,
            "total": mat_prices["total"] + manufacture["base_cost"],
            "sell_price": sell,
            "profit_no_fees": (
                sell - mat_prices["total"] - manufacture["base_cost"]
            ),
            "sales_tax": sales_tax,
            "broker_fee": broker_fee,
            "profit": (
                sell - sum([
                    mat_prices["total"],
                    manufacture["base_cost"],
                    sales_tax,
                    broker_fee,
                ])
            ),
        }


rig_names_for_sale = [
    "Small Auxiliary Thrusters I",
    "Small Cargohold Optimization I",
    "Small Hyperspatial Velocity Optimizer I",
    "Small Low Friction Nozzle Joints I",
    "Small Polycarbon Engine Housing I",
    "Small Signal Focusing Kit I",
]
rigs = entity.from_name_seq(rig_names_for_sale)

rig = rigs[0].entity

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
