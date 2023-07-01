import os
import json
import pickle
from pprint import pprint
import itertools

import networkx as nx

from hxxp import Requester
from hxxp import DefaultHandlers
from authentication import EmptyToken



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


def get_regions(r):
    return _json(r.request("GET", "/universe/regions/"))


def get_region(r, region_id):
    return _json(r.request("GET", f"/universe/regions/{region_id}"))


def read_regions(path="regions.json"):
    with open(path) as f:
        return json.load(f)


def read_types(path="types.json"):
    with open(path) as f:
        return json.load(f)


#
# Entry point
# 

from universe import ItemFactory
from universe import UniverseLookup
from universe import blueprint_lookup


requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
universe = UniverseLookup(requester)

verge_vendor = 10000068
sinq_laison = 10000032
syndicate = 10000041
molden_heath = 10000028
placid = 10000048
essence = 10000064

chill_time_cost = 900
sweaty_time_cost = 4160


# Loes V - Moon 19 - Roden Shipyards Warehouse
start_position = (30005300, 60010363)
regions = [verge_vendor, essence, placid]


# desired = {
#     (1, "small processor overclocking unit i"),
#     (1, "1mn y-s8 compact afterburner"),
#     (1, "damage control i"),
#     (1, "small cap battery i"),
#     (1, "small i-a enduring armor repairer"),
#     (1, "drone damage amplifier i"),
#     (1, "small capacitor control circuit i"),
#     (1, "small transverse bulkhead i"),
#     (1, "f-12 enduring tracking computer"),
#     (5, "125mm prototype gauss gun"),
# }

# desired = {
#     (11, "tritanium"),
#     (12, "pyerite"),
#     (5, "nocxium"),
#     (28, "mexallon"),
#     (11, "isogen"),
# }


# desired = {
#     (400, "burned logic circuit"),
#     (300, "thruster console"),
#     (300, "charred micro circuit"),
# }


# desired = {
#     (100, "impetus console"),
#     (100, "logic circuit"),
#     (100, "micro circuit"),
#     (3600, "nocxium"),
#     (8200, "isogen"),
#     (22200, "mexallon"),
#     (55600, "tritanium"),
#     (44400, "pyerite"),
# }


desired = {
    (1, "plasma command center"),
}


# desired = {
#     (1, "lava command center"),
# }



# time_cost = chill_time_cost
time_cost = sweaty_time_cost


items = ItemFactory(requester, "types.json")


graph_data = slurp_pickle("graph.pkl")

g = nx.Graph()

for edge in graph_data["edges"]:
    g.add_edge(*edge)



