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
        return f"{self.name} [id: {self.id}]"


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
            entries = _json(
                self.requester.request(
                    "POST",
                    "/universe/names",
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

    except KeyError:
        data = universe.details("system", name=name)
        # Pick just any station in the system
        return (data["system_id"], next(data["stations"]))

    else:
        raise LookupError(name)


def blueprint_lookup(entity_id):
    return _json(
        requests.get(
            "https://www.fuzzwork.co.uk/blueprint/api/blueprint.php",
            params={"typeid": entity_id},
        )
    )
