import os
import json
import pickle
from pprint import pprint
import itertools
import time

import networkx as nx

from hxxp import Requester
from hxxp import DefaultHandlers
from authentication import EmptyToken
import authentication as auth



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
tok.get()
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

from universe import UserAssets
from universe import UniverseLookup

universe = UniverseLookup(r0)
ua = UserAssets(r, "Mola Pavonis")
