import itertools
from cytoolz import valmap
from cytoolz import curry
import diskcache

from weighted_series import WeightedSeries
from requests import HTTPError

from hxxp import DefaultHandlers


_json = DefaultHandlers.raise_or_return_json


def get_orders(requester, query, region_id, type_id=None, page=1):
    params = {
        **query,
        "page": page,
    }

    if type_id is not None:
        params["type_id"] = type_id

    return _json(
        requester.request("GET", f"/markets/{region_id}/orders", params=params)
    )


def iter_orders(r, query, region_id, type_id=None):
    page = 1
    while True:
        try:
            yield from get_orders(
                r,
                query,
                region_id=region_id,
                type_id=type_id,
                page=page,
            )
            page += 1
        except HTTPError:
            return


def orders_for_item(requester, region_ids, item_id):
    return itertools.chain.from_iterable(
        iter_orders(
            requester,
            {"order_type": "all"},
            region_id,
            int(item_id),
        )
        for region_id in region_ids
    )


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
        region = self.universe.chain(
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
