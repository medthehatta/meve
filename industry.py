import requests
from cytoolz import valmap
from cytoolz import mapcat
import diskcache

from market import EveMarketMetrics
from weighted_series import WeightedSeriesMetrics
from hxxp import DefaultHandlers
from formal_vector import FormalVector


_json = DefaultHandlers.raise_or_return_json


def expand_lst(func, pred, lst):
    expanded = lst[:]
    while any(pred(x) for x in expanded):
        expanded = list(
            mapcat(lambda x: func(x) if pred(x) else [x], expanded)
        )
    return expanded


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

    def craft_type(self, entity):
        data = self.lookup(entity)
        return (
            "manufacturing" if data.get("activityMaterials", {}).get("1") else
            "reactions" if data.get("activityMaterials", {}).get("11") else
            "unknown"
        )

    def _ingredient_triples(self, entity):
        data = self.lookup(entity)

        if "activityMaterials" not in data:
            return []

        if (
            "1" in data["activityMaterials"] and
            "11" in data["activityMaterials"]
        ):
            print(
                f"Found both manufacturing and reaction processes for "
                f"{entity}; assuming manufacturing"
            )
            materials = data["activityMaterials"]["1"]
        elif "1" in data["activityMaterials"]:
            materials = data["activityMaterials"]["1"]
        elif "11" in data["activityMaterials"]:
            materials = data["activityMaterials"]["11"]
        else:
            return []

        return [
            (
                x["name"],
                x["quantity"],
                self.entities.strict.from_id(x["typeid"]),
            )
            for x in materials
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

    def craft_type(self, entity):
        return self.blueprints.craft_type(entity)

    def craft(self, item, facility, alpha=False, recurse=None):
        recurse = recurse or []
        facility_costs = self._facility_costs(facility, alpha=alpha)
        return self._craft(1, item, facility_costs, recurse)

    def _craft(self, quantity, item, facility_costs, recurse=None):
        recurse = recurse or []
        ingredients_per_unit = self.ingredients(item)
        craft_type = self.craft_type(item)
        if ingredients_per_unit == ingredients_per_unit.zero():
            raise ValueError(f"{item} is not craftable")
        ingredients = quantity * ingredients_per_unit
        parts_to_make = [
            (n, quantity, part)
            for (n, quantity, part) in ingredients.triples()
            if part in recurse
        ]
        raw_ingredients = (
            ingredients - ingredients.from_triples(parts_to_make)
        )
        crafted_parts = {
            part: self._craft(
                quantity,
                part,
                facility_costs,
                recurse=recurse,
            )
            for (n, quantity, part) in parts_to_make
        }

        installation = self._installation_cost(ingredients, facility_costs)

        return {
            "item": item,
            "runs": quantity,
            "craft_type": craft_type,
            "ingredients": ingredients_per_unit,
            "raw_ingredients": raw_ingredients + ingredients.sum(
                part["raw_ingredients"] for part in crafted_parts.values()
            ),
            "installation": installation,
            "crafted_parts": crafted_parts,
            "base_cost": installation["base_cost"] + sum(
                part["base_cost"] for part in crafted_parts.values()
            ),
        }

    def _ingredient_prices(self, ingredients):
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

    def _installation_cost(
        self,
        ingredients,
        facility_costs,
    ):
        ingredient_prices = self._ingredient_prices(ingredients)
        item_value_est = ingredient_prices["total"]
        cost = item_value_est * (
            facility_costs["cost_index"] * facility_costs["bonuses"] +
            facility_costs["tax"] +
            facility_costs["scc_percent"]/100 +
            (0.25/100 if facility_costs["alpha"] else 0)
        )

        return {
            "ingredients": ingredients,
            "ingredient_base_prices": ingredient_prices,
            "base_cost": cost,
            "facility_costs": facility_costs,
        }

    def _facility_costs(
        self,
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
        facility = self.facility(facility_entity)
        if facility is None:
            raise ValueError(
                f"Facility '{facility_entity}' does not provide manufacturing"
            )
        facility_tax = facility.get("tax", 0.25/100)
        scc_pct = 1.5  # SCC surcharge

        # TODO: figure out bonuses
        bonuses = 1

        return {
            "system": system_entity,
            "facility": facility,
            "cost_index": cost_index,
            "tax": facility_tax,
            "scc_percent": scc_pct,
            "bonuses": bonuses,
            "alpha": alpha,
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

    def craft_metrics(self, entity, alpha=False):
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
        recurse=None,
    ):
        craft = self.industry.craft(
            entity,
            self.mfg_station,
            alpha=alpha,
            recurse=recurse,
        )
        ingredients = craft["raw_ingredients"]
        prices = valmap(
            ingredient_buy_metric,
            self.ingredients_buy(ingredients),
        )
        mat_prices = {
            entity: {
                "individual": prices[entity],
                "job": quantity * prices.get(entity) if prices.get(entity) else None,
            }
            for (_, quantity, entity) in ingredients.triples()
        }
        if all(v is not None for v in prices.values()):
            mat_prices["total"] = sum(entry["job"] for entry in mat_prices.values())
        else:
            mat_prices["total"] = None
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
            "craft": craft,
            "materials": mat_prices,
            "total": (
                mat_prices["total"] + craft["base_cost"]
                if mat_prices["total"] is not None else None
            ),
            "sell_price": sell,
            "profit_no_fees": (
                sell - mat_prices["total"] - craft["base_cost"]
                if mat_prices["total"] is not None else None
            ),
            "sales_tax": sales_tax,
            "broker_fee": broker_fee,
            "profit": (
                sell - sum([
                    mat_prices["total"],
                    craft["base_cost"],
                    sales_tax,
                    broker_fee,
                ])
                if mat_prices["total"] is not None else None
            ),
        }
