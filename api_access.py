import os

from hxxp import Requester
from authentication import EmptyToken
import authentication as auth


client_id = "2ca75dd163354b0794cca4726d631df4"
with open("eve_client_secret", "r") as f:
    client_secret = f.read()

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
        "esi-industry.read_character_jobs.v1",
        "esi-universe.read_structures.v1",
        "esi-skills.read_skills.v1",
        "esi-skills.read_skillqueue.v1",
    ],
    code_fetcher=auth.get_code_http(8080),
    disk_path="token.pkl",
)

requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
authed_requester = Requester("https://esi.evetech.net/latest/", tok)
