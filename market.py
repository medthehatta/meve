import itertools
from cytoolz import valmap
from cytoolz import curry
import diskcache

from weighted_series import WeightedSeries
from requests import HTTPError

from hxxp import DefaultHandlers
from hxxp import Requester
from authentication import EveOnlineFlow


_json = DefaultHandlers.raise_or_return_json


def get_structure_orders(requester, structure_id, page=1):
    return requester.request("GET", f"/markets/structures/{structure_id}", params={"page": page})


def iter_structure_orders(requester, structure_id):
    page = 1
    while True:
        res = get_structure_orders(requester, structure_id, page=page)
        if res.ok:
            yield from res.json()
        elif res.status_code == 404 or res.status_code == 500:
            return
        else:
            print(res.__dict__)
            yield []
        page += 1


def orders_for_item(requester, region_ids, item_id):
    return itertools.chain.from_iterable(
        next(
            iter_orders(
                requester,
                {"order_type": "all"},
                region_id,
                int(item_id),
            )
        )
        for region_id in region_ids
    )


def get_orders(requester, query, region_id, type_id=None, page=1):
    params = {
        **query,
        "page": page,
    }

    if type_id is not None:
        params["type_id"] = type_id

    return requester.request("GET", f"/markets/{region_id}/orders", params=params)


def iter_orders(requester, query, region_id, type_id=None):
    page = 1
    while True:
        res = get_orders(requester, query, region_id, type_id=type_id, page=page)
        if res.ok:
            yield res.json()
        elif res.status_code == 404:
            return
        else:
            yield []
        page += 1


class OrderFetcher:

    def __init__(
        self,
        universe,
        requester,
        authed_requester=None,
        expire=60,
        disk_cache=None,
    ):
        self.universe = universe
        self.requester = requester
        self._authed_requester = authed_requester
        self.default_expire = expire
        if disk_cache:
            self._orders = diskcache.Cache(disk_cache)
        else:
            self._orders = diskcache.Cache()

    @property
    def authed_requester(self):
        if not isinstance(self._authed_requester, Requester):
            raise ValueError(
                "No authed requester present.  "
                "Re-instantiate with a valid authed requester"
            )
        if not isinstance(self._authed_requester.token, EveOnlineFlow):
            raise ValueError(
                "Authed requester is not configured with a valid token.  "
                "Re-instantiate with a valid authed requester"
            )
        return self._authed_requester

    def get_for_structure(self, entity, structure, expire=None):
        expire = expire or self.default_expire
        key = (entity.id, structure.id)

        struct_key = ("struct", structure.id)
        if struct_key not in self._orders:
            orders = iter_structure_orders(
                self.authed_requester,
                structure.id,
            )
            orders_by_key = {}
            for order in orders:
                print(order)
                key = (order["type_id"], structure.id)
                if key not in orders_by_key:
                    orders_by_key[key] = []
                else:
                    orders_by_key[key].append(order)
            for (key, orders_for_key) in orders_by_key.items():
                self._orders.set(key, orders_for_key, expire=expire)
            self._orders.set(struct_key, True, expire=expire)

        return self._orders.get(key, [])

    def get_for_station(self, entity, station, expire=None):
        expire = expire or self.default_expire
        region = self.universe.chain(
            station, "station", "system", "constellation", "region",
        )
        key = (entity.id, region.id)

        if key not in self._orders:
            seq = orders_for_item(self.requester, [region.id], entity.id)
            self._orders.set(key, list(seq), expire=expire)

        return self._orders.get(key, [])

    def get_for_regions(self, entity, regions, expire=None):
        expire = expire or self.default_expire
        for region in regions:
            key = (entity.id, region.id)

            if key not in self._orders:
                seq = orders_for_item(self.requester, [region.id], entity.id)
                self._orders.set(key, list(seq), expire=expire)

        keys = [(entity.id, region.id) for region in regions]
        return list(
            itertools.chain.from_iterable(self._orders[k] for k in keys)
        )


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


class OrderCalc:

    def __init__(self, broker_fee_percent=3, accounting_level=0):
        self.broker_fee_percent = broker_fee_percent
        self.accounting_level = accounting_level

    def sale_cost(self, price):
        sales_tax_rate = 8*(1 - 0.11/100 * self.accounting_level)/100
        sales_tax = sales_tax_rate * price
        broker_fee = max(100, self.broker_fee_percent/100 * price)
        return {
            "total": sales_tax + broker_fee,
            "sales_tax": sales_tax,
            "broker_fee": broker_fee,
        }
