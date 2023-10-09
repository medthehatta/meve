import datetime

from hxxp import DefaultHandlers
from tracked_map import TrackedMap


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

    def __init__(self, requester, character_name):
        self.requester = requester
        self.character_name = character_name
        self._character_id = None

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

    def wallet_journal(self):
        data = _json(
            self.requester.request(
                "GET",
                f"/characters/{self.character_id}/wallet/journal",
            )
        )
        return data

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

    def orders(self):
        return _json(
            self.requester.request(
                "GET",
                f"/characters/{self.character_id}/orders",
            )
        )

    def jobs(self):
        return _json(
            self.requester.request(
                "GET",
                f"/characters/{self.character_id}/industry/jobs",
            )
        )

    def total_quantities(self):
        return self.aggregate_on_field("quantity", self.assets())

    def aggregate_on_field(
        self,
        field,
        data,
        type_field="type_id",
        agg=lambda x, y: x + y,
    ):
        results = {}
        for datum in data:
            type_id = datum[type_field]
            results[type_id] = agg(results.get(type_id, 0), datum[field])
        return results
