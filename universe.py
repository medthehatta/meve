import itertools
import json
import diskcache
from functools import wraps
import time

from cytoolz import mapcat
import requests

from hxxp import DefaultHandlers


_json = DefaultHandlers.raise_or_return_json

UNSET = object()


def _dbg(x, f, sleep=0):
    print(f.format(x=x))
    time.sleep(sleep)
    return x


def expand_lst(func, pred, lst):
    expanded = lst[:]
    while any(pred(x) for x in expanded):
        expanded = list(
            mapcat(lambda x: func(x) if pred(x) else [x], expanded)
        )
    return expanded


class Entity:

    def __init__(self, entity_id, name):
        self.id = int(entity_id) if entity_id is not None else None
        self.name = name

    def __repr__(self):
        return f"{self.name or '???'} [id: {self.id or '???'}]"

    def __eq__(self, other):
        return (
            (
                isinstance(other, type(self)) or
                isinstance(self, type(other))
            ) and
            self.id == other.id
        )

    def __hash__(self):
        return hash(self.id)


class EntityStrictEvaluator:

    def __init__(self, factory):
        self.factory = factory

    def __getattr__(self, attr):
        found = getattr(self.factory, attr)

        if callable(found):

            @wraps(found)
            def _wrapped(*args, **kwargs):
                return self.factory.reify(found(*args, **kwargs))

            return _wrapped

        else:
            return found


class EntityFactory:

    @classmethod
    def reify(cls, struct):
        if isinstance(struct, dict):
            return type(struct)({k: cls.reify(v) for (k, v) in struct.items()})
        elif isinstance(struct, (list, tuple)):
            return type(struct)([cls.reify(x) for x in struct])
        elif isinstance(struct, LazyEntity):
            return struct.entity
        else:
            return struct

    def __init__(self, items, universe):
        self.items = items
        self.universe = universe
        self.strict = EntityStrictEvaluator(self)

    def from_name(self, name):
        return LazyEntity(self.universe, self.items, name=name)

    def named(self, name):
        return self.from_name(name)

    def from_id(self, id):
        return LazyEntity(self.universe, self.items, id=id)

    def from_name_seq(self, names):
        return [self.from_name(name) for name in names]

    def from_id_seq(self, ids):
        return [self.from_id(eid) for eid in ids]

    def from_names(self, *names):
        return [self.from_name(name) for name in names]

    def from_ids(self, *ids):
        return [self.from_id(eid) for eid in ids]

    def __call__(self, **kwargs):
        return LazyEntity(self.universe, self.items, **kwargs)


class LazyEntity(Entity):

    def __init__(self, universe, items, entity=None, name=None, id=None):
        self.universe = universe
        self.items = items
        if not any([entity, name, id]):
            raise TypeError("Must provide entity, name, or id")
        self._entity = entity
        self._name = name
        self._id = int(id) if id is not None else None

    @property
    def entity(self):
        if self._entity:
            pass
        elif self._id:
            self._entity = self.universe.from_id(self._id)
        elif self._name:
            try:
                self._entity = self.universe.from_name(self._name)
            except LookupError:
                self._entity = self.items.from_terms(self._name)
        else:
            raise ValueError(self.__dict__)

        return self._entity

    @property
    def id(self):
        if self._id is not None:
            return self._id
        else:
            return self.entity.id

    @property
    def name(self):
        if self._name is not None:
            return self._name
        else:
            return self.entity.name

    def __repr__(self):
        if self._entity:
            return repr(self._entity)
        elif self._name:
            return f'LazyEntity(name="{self._name}")'
        elif self._id:
            return f'LazyEntity(id="{self._id}")'
        else:
            raise ValueError(self.__dict__)


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

    def chain_seq(self, entity, k_chain, default=UNSET):
        ka_chain = [(a, f"{b}_id") for (a, b) in zip(k_chain, k_chain[1:])]

        found = entity.id

        for (kind, attr) in ka_chain:
            try:
                found = self.details(kind, entity_id=found)[attr]
            except KeyError:
                if default is UNSET:
                    raise
                else:
                    found = default
                    break

        return self.from_id(found)

    def chain(self, entity, *k_chain, default=UNSET):
        return self.chain_seq(entity, k_chain, default=default)


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
