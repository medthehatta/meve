import itertools
import json
import diskcache

import requests

from hxxp import DefaultHandlers


_json = DefaultHandlers.raise_or_return_json


class Entity:

    def __init__(self, entity_id, name):
        self.id = entity_id
        self.name = name

    def __repr__(self):
        return f"{self.name or '???'} [id: {self.id or '???'}]"


class ItemFactory:

    def __init__(self, requester, type_repo_path):
        self.requester = requester
        with open(type_repo_path, "r") as f:
            # Fix the types; downloaded repo has strings instead of ints for
            # the type ids
            self.type_repo = {k: int(v) for (k, v) in json.load(f).items()}
        with open(type_repo_path, "r") as f:
            self.type_repo_rev = {int(v): k for (k, v) in json.load(f).items()}

    def from_terms(self, terms):
        exact = next(
            (t for t in self.type_repo if terms.lower() == t.lower()),
            None,
        )
        if exact:
            return Entity(entity_id=self.type_repo[exact], name=exact)
        else:
            matches = [
                t for t in self.type_repo
                if all(y.lower() in t.lower() for y in terms.split())
            ]
            if len(matches) == 0:
                raise LookupError("No matches.")
            elif len(matches) == 1:
                t = matches[0]
                return Entity(entity_id=self.type_repo[t], name=t)
            else:
                raise LookupError(f"Ambiguous matches for '{terms}' (next line):\n{matches}")

    def from_name(self, name):
        return self.type_repo[name]

    def from_id(self, entity_id):
        return self.type_repo_rev[int(entity_id)]


class UniverseLookup:

    def __init__(self, requester):
        self.requester = requester
        self.cache = diskcache.Cache("eve_universe_names")

    def from_names(self, names):
        found = {}
        for name in names:
            if ("name", name) in self.cache:
                found[name] = self.cache.get(("name", name))
        missing = list(set(name for name in names if name not in found))
        if missing:
            entries = _json(
                self.requester.request(
                    "POST",
                    "/universe/ids",
                    json=missing,
                )
            )

            if isinstance(entries, (list, tuple)):
                seq = entries
            elif isinstance(entries, dict):
                seq = itertools.chain.from_iterable(entries.values())
            else:
                raise ValueError(f"Unknown entity response type: {entries}")

            for entry in seq:
                self.cache.set(("id", entry["id"]), entry["name"])
                self.cache.set(("name", entry["name"]), entry["id"])
                found[entry["name"]] = entry["id"]

        return [Entity(entity_id=found[name], name=name) for name in names]

    def from_name(self, name):
        return self.from_names([name])[0]

    def from_ids(self, ids):
        found = {}
        for id_ in ids:
            if ("id", id_) in self.cache:
                found[id_] = self.cache.get(("id", id_))
        missing = list(set(id_ for id_ in ids if id_ not in found))
        if missing:
            res = self.requester.request(
                "POST",
                "/universe/names",
                json=missing,
            )
            if not res.ok:
                entries = [{"id": id_, "name": None} for id_ in missing]
            else:
                entries = _json(res)

            if isinstance(entries, (list, tuple)):
                seq = entries
            elif isinstance(entries, dict):
                seq = itertools.chain.from_iterable(entries.values())
            else:
                raise ValueError(f"Unknown entity response type: {entries}")

            for entry in seq:
                self.cache.set(("id", entry["id"]), entry["name"])
                self.cache.set(("name", entry["name"]), entry["id"])
                found[entry["id"]] = entry["name"]

        return [Entity(entity_id=id_, name=found[id_]) for id_ in ids]

    def from_id(self, id_):
        return self.from_ids([id_])[0]

    def details(self, kind, entity=None, name=None, entity_id=None):
        if entity_id:
            id_ = entity_id
        elif entity:
            id_ = entity.id
        elif name:
            id_ = self.from_name(name).id

        kind = kind.lower().rstrip("s")
        return _json(
            self.requester.request("GET", f"/universe/{kind}s/{id_}")
        )


def station_lookup(universe, name):
    try:
        data = universe.details("station", name=name)
        return (data["system_id"], data["station_id"])

    except (KeyError, requests.exceptions.HTTPError):
        data = universe.details("system", name=name)
        # Pick just any station in the system
        return (data["system_id"], next(iter(data["stations"])))

    else:
        raise LookupError(name)


def blueprint_lookup(entity_id):
    return _json(
        requests.get(
            "https://www.fuzzwork.co.uk/blueprint/api/blueprint.php",
            params={"typeid": entity_id},
        )
    )


UNSET = object()

class UserAssets:

    def __init__(self, requester, character_name):
        self.requester = requester
        self.character_name = character_name
        self.cache = diskcache.Cache("user_assets")
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

    def key(self, type_id):
        return ("smartavgbuy", self.character_id, type_id)

    def last_key(self):
        return ("smartavgbuy_last", self.character_id)

    def smart_avg_buy(self, type_id, default=UNSET):
        if self.key(type_id) in self.cache:
            return self.cache.get(self.key(type_id))
        elif default is UNSET:
            raise LookupError(type_id)
        else:
            return default

    def update_smart_avg_buy(self):
        last = self.cache.get(self.last_key(), default="")
        new_purchases = self.purchases(since=last)
        new_avgs = self._average_purchase_prices(new_purchases)
        new_amts = self._total_quantities(new_purchases)
        current_quantities = self.total_quantities()

        for k in new_avgs:
            avg_price = new_avgs[k]
            quantity = new_amts[k]
            if (
                current_quantities.get(k, 0)
                and current_quantities[k] > quantity
                and self.key(k) in self.cache
            ):
                total_spend = (
                    quantity*avg_price
                    + (current_quantities[k] - quantity)*self.smart_avg_buy(k)
                )
                self.cache.set(
                    self.key(k),
                    total_spend / current_quantities[k],
                )
            else:
                self.cache.set(
                    self.key(k),
                    avg_price,
                )
        self.cache.set(
            self.key_last(),
            max(p["date"] for p in new_purchases),
        )

    def total_quantities(self):
        data = _json(
            self.requester.request("GET", f"/characters/{self.character_id}/assets")
        )
        return self._total_quantities(data)

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

    def _average_purchase_prices(self, purchases):
        acquired = {}
        spent = {}
        for purchase in purchases:
            type_id = purchase["type_id"]
            acquired[type_id] = acquired.get(type_id, 0) + purchase["quantity"]
            spent[type_id] = (
                spent.get(type_id, 0)
                + purchase["quantity"]*purchase["unit_price"]
            )
        avg = {k: spent[k] / acquired[k] for k in acquired}
        return avg

    def _total_quantities(self, entries):
        acquired = {}
        for entry in entries:
            type_id = entry["type_id"]
            acquired[type_id] = acquired.get(type_id, 0) + entry["quantity"]
        return acquired


