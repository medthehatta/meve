import os
from dataclasses import dataclass
import json
import pickle
from pprint import pprint
import itertools
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import networkx as nx
from cytoolz import get_in
from bs4 import BeautifulSoup

from formal_vector import FormalVector
from hxxp import Requester
from hxxp import DefaultHandlers
from authentication import EmptyToken
import authentication as auth

from requests.exceptions import JSONDecodeError


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






client_id = "2ca75dd163354b0794cca4726d631df4"
client_secret = "eHwPGFnA99aGu784pJBqv3U7mi9t6IfaNbkUmoKN"

tok = auth.EveOnlineFlow(
    "https://login.eveonline.com/v2/oauth/token",
    client_id=client_id,
    client_secret=client_secret,
    scopes=[
        "esi-wallet.read_character_wallet.v1",
        "esi-wallet.read_corporation_wallet.v1",
        "esi-assets.read_assets.v1",
        "esi-markets.structure_markets.v1",
        "esi-markets.read_character_orders.v1",
        "esi-wallet.read_corporation_wallets.v1",
        "esi-assets.read_corporation_assets.v1",
        "esi-markets.read_corporation_orders.v1",
    ],
    code_fetcher=auth.get_code_http(8080),
)
if os.path.exists("token.pkl"):
    token_data = slurp_pickle("token.pkl")
    tok.tokens = token_data["tokens"]
    tok.expire_time = token_data["expire_time"]
    tok.refresh_expire_time = token_data["refresh_expire_time"]
#tok.get()
dump_pickle(
    {
        "tokens": tok.tokens,
        "expire_time": tok.expire_time,
        "refresh_expire_time": tok.refresh_expire_time,
    },
    "token.pkl",
)

r0 = Requester("https://esi.evetech.net/latest/", EmptyToken())
r = Requester("https://esi.evetech.net/latest/", tok)
zk = Requester("https://zkillboard.com", EmptyToken())


from universe import UserAssets
from universe import UniverseLookup
from universe import ItemFactory
from universe import BlueprintLookup
from universe import Ingredients
from purchase_tour import orders_by_location
from purchase_tour import orders_by_item
from purchase_tour import orders_in_regions
from cli import DEFAULT_REGION_NAMES
from delayed import Delayed


universe = UniverseLookup(r0)
items = ItemFactory(r0, "types.json")
blueprints = BlueprintLookup(items)
#ua = UserAssets(r, "Mola Pavonis")
ingredients_parser = lambda s: Ingredients.parse_with_item_lookup(s, items=items)


def kills_soup(character_name):
    character_id = universe.from_name(character_name).id
    return BeautifulSoup(zk.request("GET", f"/character/{character_id}/").text, features="html.parser")


def pad_lst_to_len(length, value=None):

    def _pad_lst_to_len(lst):
        current = len(lst)
        padding = [value]*(length - current)
        return lst + padding

    return _pad_lst_to_len
    

def killdata(character_name):
    pad = pad_lst_to_len(6, "")

    soup = kills_soup(character_name)

    danger_percents = [x.text for x in soup.find_all("div", attrs={"class": "progress-bar-danger"})]
    snuggly_percents = [
        x.text for x in soup.find_all("div", attrs={"class": "progress-bar"})
        if "progress-bar-danger" not in x.attrs.get("class", [])
    ]

    danger_numbered = [
        int(x.strip().strip("%")) if x.strip() else 0 for x in danger_percents
    ]
    snuggly_numbered = [
        int(x.strip().strip("%")) if x.strip() else 0 for x in snuggly_percents
    ]

    percents = list(zip(pad(danger_numbered), pad(snuggly_numbered)))

    dangerous = [
        danger if danger else (100 - snuggly) if snuggly else 0
        for (danger, snuggly) in percents
    ]

    match dangerous:

        case [da, _, None, *_]:
            (danger_recent, danger_all) = (0, da)

        case [da, _, dr, *_]:
            (danger_recent, danger_all) = (dr, da)

        case _:
            (danger_recent, danger_all) = (0, 0)

    

    return (danger_recent, danger_all)


class Mapper:

    def map(self, func, seq):
        for s in seq:
            yield func(s)


@contextmanager
def FlatExecutor(*args, **kwargs):
    yield Mapper()


def killdata_from_stream(stream):
    keys = [x for x in (line.strip() for line in stream) if x]
    with ThreadPoolExecutor(max_workers=6) as exe:
        values = list(exe.map(killdata, keys))

    return dict(zip(keys, values))


def ranked_threats(data):
    return sorted(((v, k) for (k, v) in data.items()), reverse=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--seconds", type=int, default=10)
    parser.add_argument("path")
    parsed = parser.parse_args()

    path = parsed.path
    seconds = parsed.seconds

    last_contents = tuple([])

    while True:
        with open(path, "r") as fh:
            contents = tuple(line for line in fh if line.strip())
        if contents == last_contents:
            time.sleep(seconds)
            continue
        else:
            print("\n\n\n")
            pprint(ranked_threats(killdata_from_stream(contents)))
            last_contents = contents
            time.sleep(seconds)


if __name__ == "__main__":
    main()
