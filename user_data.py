import json
from functools import reduce

from hxxp import DefaultHandlers
from saved_accumulator import SavedAccumulator


_json = DefaultHandlers.raise_or_return_json

UNSET = object()


def rolling_weighted_average(acc, seq):
    (weight, value) = acc

    total_weights = weight
    total = weight * value
    for record in seq:

        match record:

            case (w, v):
                total += w * v
                total_weights += w

    return total / total_weights


def rolling_weighted_dict_average(acc, seq):
    values = {key: acc[key][1] for key in acc}
    weights = {key: acc[key][0] for key in acc}

    total = {key: weights[key] * values[key] for key in values}
    total_weights = {key: weights[key] for key in weights}

    for record in seq:

        match record:

            case {"weight": weight, "value": value, "key": key}:
                total[key] = total.get(key, 0) + weight * value
                total_weights[key] = total_weights.get(key, 0) + weight

    return {key: total[key] / total_weights[key] for key in values}


class UserAssets:

    def __init__(self, requester, character_name, path=None):
        self.requester = requester
        self.character_name = character_name
        self._character_id = None
        self._smart_avg_buy = SavedAccumulator(
            accumulator=self._accum_avg,
            path=f"{character_name}-avg-buy.uadb",
            dumper=json.dump,
            loader=json.load,
        )
        self._smart_avg_sell = SavedAccumulator(
            accumulator=self._accum_avg,
            path=f"{character_name}-avg-sell.uadb",
            dumper=json.dump,
            loader=json.load,
        )

    @property
    def character_id(self):
        if self._character_id is None:
            data = _json(
                self.requester.request(
                    "POST",
                    "/universe/ids",
                    json=[self.character_name],
                )
            )
            found = data["characters"]
            if len(found) == 1:
                self._character_id = data["characters"][0]["id"]
            else:
                raise LookupError(f"Expected one match but got: {found}")
        return self._character_id

    def assets(self):
        return _json(
            self.requester.request("GET", f"/characters/{self.character_id}/assets")
        )

    def transactions(self):
        data = _json(
            self.requester.request(
                "GET",
                f"/characters/{self.character_id}/wallet/transactions",
            )
        )
        return data

    def purchases(self, since=None, until=None):
        transactions = self.transactions()
        return [
            {
                "date": x["date"],
                "type_id": x["type_id"],
                "quantity": x["quantity"],
                "unit_price": x["unit_price"],
            }
            for x in transactions
            if (
                x["is_buy"]
                and (x["date"] >= since if since else True)
                and (x["date"] <= until if until else True)
            )
        ]

    def sales(self, since=None, until=None):
        transactions = self.transactions()
        return [
            {
                "date": x["date"],
                "type_id": x["type_id"],
                "quantity": x["quantity"],
                "unit_price": x["unit_price"],
            }
            for x in transactions
            if (
                not x["is_buy"]
                and (x["date"] >= since if since else True)
                and (x["date"] <= until if until else True)
            )
        ]

    def _accum_avg(self, acc, seq):
        return rolling_weighted_dict_average(
            acc,
            (
                {
                    "weight": x["quantity"],
                    "value": x["unit_price"],
                    "key": x["type_id"],
                }
                for x in seq
            ),
        )

    def smart_avg_buy(self, type_id, default=UNSET):
        data = self._smart_avg_buy.read()
        if type_id in data:
            return data[type_id]
        elif default is UNSET:
            raise LookupError(type_id)
        else:
            return default

    def smart_avg_sell(self, type_id, default=UNSET):
        data = self._smart_avg_sell.read()
        if type_id in data:
            return data[type_id]
        elif default is UNSET:
            raise LookupError(type_id)
        else:
            return default

    def update_aggregates(self):
        self._smart_avg_buy.aggregate(self.purchases())
        self._smart_avg_sell.aggregate(self.sales())

    def total_quantities(self):
        entries = self.assets()
        acquired = {}
        for entry in entries:
            type_id = entry["type_id"]
            acquired[type_id] = acquired.get(type_id, 0) + entry["quantity"]
        return acquired
