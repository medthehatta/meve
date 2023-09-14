import itertools
import json
import diskcache
from functools import wraps
from functools import partial
import time

from cytoolz import iterate
from cytoolz import mapcat
import requests

from hxxp import DefaultHandlers
from formal_vector import FormalVector


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
        return self.id == other.id


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


class Ingredients(FormalVector):

    _ZERO = "Ingredients.NONE"

    @classmethod
    def parse_with_item_lookup(cls, s, items=None, fuzzy=False):
        if items:

            def _normalize(x):
                entity = items.from_terms(x)
                return entity.name

            def _populate(x):
                entity = items.from_terms(x)
                return entity

        else:
            _normalize = None
            _populate = None

        return super().parse(
            s, populate=_populate, normalize=_normalize, fuzzy=fuzzy,
        )


    @classmethod
    def parse_from_list_with_item_lookup(cls, lst, items=None, fuzzy=False):
        if items:

            def _normalize(x):
                entity = items.from_terms(x)
                return entity.name

            def _populate(x):
                entity = items.from_terms(x)
                return entity

        else:
            _normalize = None
            _populate = None

        return super().parse_from_list(
            lst, populate=_populate, normalize=_normalize, fuzzy=fuzzy,
        )

    def from_entities(cls, entities):
        return cls.from_triples(
            [(entity.name, 1, entity) for entity in entities]
        )

    def from_entity(cls, entity):
        return cls.from_entities([entity])

    def pretty(self):
        return "\n".join(
            f"{amount} {name}"
            for (name, amount, _) in self.triples()
        )


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


class BlueprintLookup:

    def __init__(self, items, entities):
        self.cache = diskcache.Cache("eve_blueprints")
        self.entities = entities

    def lookup(self, entity):
        entity_id = entity.id

        if entity_id not in self.cache:
            result = _json(
                requests.get(
                    "https://www.fuzzwork.co.uk/blueprint/api/blueprint.php",
                    params={"typeid": entity_id},
                )
            )
            self.cache.set(entity_id, result)

        return self.cache.get(entity_id)

    def _ingredient_triples(self, entity):
        data = self.lookup(entity)

        if "activityMaterials" not in data:
            return []

        return [
            (
                x["name"],
                x["quantity"],
                self.entities.strict.from_id(x["typeid"]),
            )
            for x in data["activityMaterials"]["1"]
        ]

    def ingredients(self, entity, recurse=None):
        if recurse is not None:
            recurse = recurse + [entity]
        else:
            recurse = [entity]

        def _recurse_triples(t):
            (n, q, e) = t
            return [
                (n1, q*q1, e1)
                for (n1, q1, e1) in self._ingredient_triples(e)
            ]

        triples = expand_lst(
            _recurse_triples,
            lambda x: x[-1] in recurse,
            [(entity.name, 1, entity)],
        )
        return Ingredients.from_triples(triples)


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


class Industry:

    def __init__(self, universe, blueprints):
        self.universe = universe
        # FIXME: Dumb but we can fix all the dependency injection later
        self.requester = self.universe.requester
        self.blueprints = blueprints
        self._cost_indices = None
        self._market_prices = None
        self._facility_info = None

    def ingredients(self, entity):
        return self.blueprints.ingredients(entity)

    def base_manufacture_cost_verbose(self, entity):
        ingredients = self.blueprints.ingredients(entity)
        pre_price = (
            (name, quantity, self.adjusted_price(e), e)
            for (name, quantity, e) in ingredients.triples()
        )
        ingredient_prices = {
            e: {
                "individual": adjusted_price,
                "job": quantity * adjusted_price,
            }
            for (_, quantity, adjusted_price, e) in pre_price
        }
        ingredient_prices["total"] = sum(
            x["job"] for x in ingredient_prices.values()
        )
        return ingredient_prices

    def base_manufacture_cost(self, entity):
        return self.base_manufacture_cost_verbose(entity)["total"]

    def installation_cost_verbose(
        self,
        item_entity,
        facility_entity,
        alpha=False,
    ):
        system_entity = self.universe.chain(
            facility_entity,
            "station",
            "system",
        )
        cost_index = self.manufacturing_index(system_entity)
        if not cost_index:
            raise ValueError(
                f"System '{system_entity}' does not provide manufacturing"
            )
        ingredient_prices = self.base_manufacture_cost_verbose(item_entity)
        # TODO: figure out bonuses
        bonuses = 1
        facility = self.facility(facility_entity)
        if facility is None:
            raise ValueError(
                f"Facility '{facility_entity}' does not provide manufacturing"
            )
        facility_tax = facility.get("tax", 0.25/100)
        cost = ingredient_prices["total"] * (
            cost_index * bonuses +
            facility_tax +
            0.25/100 +
            (0.25/100 if alpha else 0)
        )
        return {
            "item": item_entity,
            "facility": facility_entity,
            "facility_info": facility,
            "alpha": alpha,
            "ingredients": ingredient_prices,
            "base_cost": cost,
        }

    def adjusted_price(self, item_entity):
        return self.market_prices()["adjusted"][item_entity.id]

    def manufacturing_index(self, system_entity):
        return self.cost_indices()["manufacturing"].get(system_entity.id)

    def facility(self, facility_entity):
        return self.facility_info().get(facility_entity.id)

    def cost_indices(self):
        if self._cost_indices is None:
            requester = self.requester
            data = _json(requester.request("GET", "/industry/systems/"))
            result = {}

            for system in data:
                sid = system["solar_system_id"]
                for idx in system["cost_indices"]:
                    act = idx["activity"]
                    if act not in result:
                        result[act] = {}
                    result[act][sid] = idx["cost_index"]

            self._cost_indices = result

        return self._cost_indices

    def market_prices(self):
        if self._market_prices is None:
            requester = self.requester
            data = _json(requester.request("GET", "/markets/prices/"))
            result = {
                "adjusted": {},
                "average": {},
            }

            for entry in data:
                result["adjusted"][entry["type_id"]] = entry["adjusted_price"]
                result["average"][entry["type_id"]] = entry.get("average_price")

            self._market_prices = result

        return self._market_prices

    def facility_info(self):
        if self._facility_info is None:
            requester = self.requester
            data = _json(requester.request("GET", "/industry/facilities/"))
            result = {
                entry["facility_id"]: entry for entry in data
            }

            self._facility_info = result

        return self._facility_info


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
            self.last_key(),
            max(p["date"] for p in new_purchases),
        )

    def assets(self):
        return _json(
            self.requester.request("GET", f"/characters/{self.character_id}/assets")
        )

    def total_quantities(self):
       return self._total_quantities(self.assets())

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
