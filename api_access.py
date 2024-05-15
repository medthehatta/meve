import os

from hxxp import Requester
from authentication import EmptyToken
import authentication as auth


client_id = "05b94fc680cc4eccb2b500d1db696077"
with open("eve_client_secret", "r") as f:
    client_secret = f.read().strip()

tok = auth.EveOnlineFlow(
    "https://login.eveonline.com/v2/oauth/token",
    client_id=client_id,
    client_secret=client_secret,
    scopes=[
        "esi-calendar.read_calendar_events.v1",
        "esi-location.read_location.v1",
        "esi-location.read_ship_type.v1",
        "esi-skills.read_skills.v1",
        "esi-skills.read_skillqueue.v1",
        "esi-wallet.read_character_wallet.v1",
        "esi-wallet.read_corporation_wallet.v1",
        "esi-search.search_structures.v1",
        "esi-clones.read_clones.v1",
        "esi-characters.read_contacts.v1",
        "esi-universe.read_structures.v1",
        "esi-bookmarks.read_character_bookmarks.v1",
        "esi-killmails.read_killmails.v1",
        "esi-corporations.read_corporation_membership.v1",
        "esi-assets.read_assets.v1",
        "esi-planets.manage_planets.v1",
        "esi-markets.structure_markets.v1",
        "esi-corporations.read_structures.v1",
        "esi-characters.read_chat_channels.v1",
        "esi-characters.read_agents_research.v1",
        "esi-industry.read_character_jobs.v1",
        "esi-markets.read_character_orders.v1",
        "esi-characters.read_blueprints.v1",
        "esi-characters.read_corporation_roles.v1",
        "esi-location.read_online.v1",
        "esi-contracts.read_character_contracts.v1",
        "esi-clones.read_implants.v1",
        "esi-killmails.read_corporation_killmails.v1",
        "esi-corporations.track_members.v1",
        "esi-wallet.read_corporation_wallets.v1",
        "esi-corporations.read_divisions.v1",
        "esi-corporations.read_contacts.v1",
        "esi-assets.read_corporation_assets.v1",
        "esi-corporations.read_titles.v1",
        "esi-corporations.read_blueprints.v1",
        "esi-bookmarks.read_corporation_bookmarks.v1",
        "esi-contracts.read_corporation_contracts.v1",
        "esi-corporations.read_standings.v1",
        "esi-corporations.read_starbases.v1",
        "esi-industry.read_corporation_jobs.v1",
        "esi-markets.read_corporation_orders.v1",
        "esi-corporations.read_container_logs.v1",
        "esi-industry.read_character_mining.v1",
        "esi-industry.read_corporation_mining.v1",
        "esi-planets.read_customs_offices.v1",
        "esi-corporations.read_facilities.v1",
        "esi-corporations.read_medals.v1",
        "esi-corporations.read_fw_stats.v1",
    ],
    code_fetcher=auth.get_code_http(8080),
    disk_path="token.pkl",
)

requester = Requester("https://esi.evetech.net/latest/", EmptyToken())
authed_requester = Requester("https://esi.evetech.net/latest/", tok)


