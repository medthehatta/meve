import datetime
import itertools
import json
import os
import sqlite3
from textwrap import dedent

from cytoolz import groupby
from cytoolz import unique

from authentication import EmptyToken
from cli import DEFAULT_REGION_NAMES
from delayed import Delayed
from formal_vector import FormalVector
from hxxp import DefaultHandlers
from hxxp import Requester
from purchase_tour import orders_by_item
from purchase_tour import orders_by_location
from purchase_tour import orders_for_item_at_location
from purchase_tour import orders_for_item_in_system
from purchase_tour import orders_in_regions
from universe import BlueprintLookup
from universe import EntityFactory
from universe import Ingredients
from universe import ItemFactory
from universe import UniverseLookup
from universe import UserAssets
import authentication as auth

_json = DefaultHandlers.raise_or_return_json

UNSET = object()


def eve_token(path):
    token_url = "https://login.eveonline.com/v2/oauth/token"
    client_id = "2ca75dd163354b0794cca4726d631df4"
    client_secret = "eHwPGFnA99aGu784pJBqv3U7mi9t6IfaNbkUmoKN"
    scopes = [
        "esi-wallet.read_character_wallet.v1",
        "esi-wallet.read_corporation_wallet.v1",
        "esi-assets.read_assets.v1",
        "esi-markets.structure_markets.v1",
        "esi-markets.read_character_orders.v1",
        "esi-wallet.read_corporation_wallets.v1",
        "esi-assets.read_corporation_assets.v1",
        "esi-markets.read_corporation_orders.v1",
    ]

    return auth.EveOnlineFlow(
        token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
        code_fetcher=auth.get_code_http(8080),
        disk_path=path,
    )


def parse_ts(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").timestamp()


def dict_db(path):

    def dict_factory(cursor, row):
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    db = sqlite3.connect(path, isolation_level=None)
    db.row_factory = dict_factory

    return db


class SqliteWrapper:

    def __init__(self, path=":memory:"):
        self.path = path

    def get(self):
        self.con = dict_db(self.path)
        return self.con

    def execute(self, *args, **kwargs):
        return self.get().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self.get().executemany(*args, **kwargs)

    def executescript(self, script, *args, **kwargs):
        return self.get().executescript(dedent(script), *args, **kwargs)


class TrackedMap:

    def __init__(self, path, value_type=str, default=UNSET):
        self.path = path
        self.value_type = value_type
        self.default = self.value_type() if default is UNSET else default
        self.db_path = os.path.join(self.path, "db")
        self.snap_path = os.path.join(self.path, "snap")
        self.snap_ts_path = os.path.join(self.path, "snaptime")

    @property
    def db(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        return dict_db(self.db_path)

    def ensure_tables(self):
        sql = dedent(
            """
            BEGIN;

            CREATE TABLE IF NOT EXISTS updates (timestamp REAL, key TEXT, value TEXT);
            CREATE INDEX IF NOT EXISTS idx_timestamp_updates ON updates(timestamp);
            CREATE INDEX IF NOT EXISTS idx_key_updates ON updates(key);

            COMMIT;
            """
        )
        with self.db as db:
            db.executescript(sql)

    def read_snap(self):
        if not os.path.exists(self.snap_path):
            return {}
        with open(self.snap_path, "r") as f:
            return json.load(f)

    def write_snap(self, data, timestamp=None):
        timestamp = timestamp or datetime.datetime.now().timestamp()
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        with open(self.snap_ts_path, "w") as f:
            json.dump({"timestamp": timestamp}, f)
        with open(self.snap_path, "w") as f:
            return json.dump(data, f)

    def last_snap_time(self, default=-1):
        if not os.path.exists(self.snap_ts_path):
            return default
        with open(self.snap_ts_path, "r") as f:
            return json.load(f).get("timestamp", default)

    def differences(self, data):
        snap = self.read_snap()
        all_keys = unique(itertools.chain(data.keys(), snap.keys()))
        return {
            k: data.get(k, self.default) for k in all_keys
            if data.get(k) != snap.get(k)
        }

    def to_row_seq(self, data, **meta):
        return [
            {**meta, "key": k, "value": str(v)}
            for (k, v) in data.items()
        ]

    def record(self, data):
        self.ensure_tables()
        now = datetime.datetime.now().timestamp()
        delta = self.differences(data)
        self.write_snap(data, timestamp=now)
        with self.db:
            self.db.executemany(
                (
                    "INSERT INTO updates (timestamp, key, value) "
                    "VALUES (:ts, :key, :value);"
                ),
                self.to_row_seq(delta, ts=now),
            )
            self.db.commit()

    def timeseries_for_key(self, key, days_back=60):
        self.ensure_tables()
        now = datetime.datetime.now().timestamp()
        before = now - days_back * (24*3600)
        with self.db:
            res = self.db.execute(
                (
                    "SELECT timestamp,value FROM updates WHERE "
                    "key=:key AND timestamp > :before;"
                ),
                {"key": key, "before": before},
            ).fetchall()
        return [
            {
                **record,
                "value": (
                    self.value_type(record["value"])
                    if record["value"] is not None else self.default
                ),
            }
            for record in res
        ]


# class WatcherDB(SqliteWrapper):
# 
#     def __init__(self, requester, character_id, path=":memory:"):
#         super().__init__(path)
#         self.requester = requester
#         self.character_id = character_id
# 
#     def ensure_tables(self):
#         sql = """
#         BEGIN;
# 
#         CREATE TABLE IF NOT EXISTS assets(character_id INTEGER, item_id INTEGER, ts REAL, quantity INTEGER);
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_item_assets ON assets(character_id, item_id);
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_ts_assets ON assets(ts);
# 
#         CREATE TABLE IF NOT EXISTS latest_assets(character_id INTEGER, item_id INTEGER, quantity INTEGER);
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_item_latest_assets ON latest_assets(character_id, item_id);
# 
#         CREATE TABLE IF NOT EXISTS asset_checkpoints(character_id INTEGER, end_ts REAL, start_ts REAL);
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_character_id_end_ts_asset_checkpoints ON asset_checkpoints(character_id, end_ts);
# 
#         CREATE TABLE IF NOT EXISTS transactions(
#             character_id INTEGER,
#             item_id INTEGER,
#             ts REAL,
#             quantity INTEGER,
#             unit_price REAL,
#             location_id INTEGER,
#             is_buy INTEGER
#         );
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_item_transactions ON transactions(character_id, item_id);
#         CREATE UNIQUE INDEX IF NOT EXISTS idx_ts_transactions ON transactions(ts);
# 
#         CREATE TABLE IF NOT EXISTS last(character_id INTEGER, table_name TEXT, ts REAL);
#         COMMIT;
#         """
#         return self.executescript(sql)
# 
#     def _update_last(self, table):
#         with self.get() as db:
#             cur = db.cursor()
#             res = cur.execute(
#                 "SELECT ts FROM last WHERE character_id=:character_id AND table_name=:table;",
#                 {"character_id": self.character_id, "table": table},
#             )
#             entries = res.fetchall()
#             if len(entries) == 1:
#                 ts = entries[0]["ts"]
#                 cur.execute(
#                     (
#                         "UPDATE last SET ts=:ts WHERE "
#                         "character_id=:character_id AND table_name=:table;"
#                     ),
#                     {
#                         "character_id": self.character_id,
#                         "ts": datetime.datetime.now().timestamp(),
#                         "table": table,
#                     },
#                 )
#                 db.close()
#             elif len(entries) == 0:
#                 ts = 0
#                 cur.execute(
#                     "INSERT INTO last VALUES (:character_id, :table, :ts);",
#                     {
#                         "character_id": self.character_id,
#                         "table": table,
#                         "ts": datetime.datetime.now().timestamp(),
#                     },
#                 )
#                 db.close()
#             else:
#                 db.close()
#                 raise ValueError(f"Found multiple entries: {entries}")
# 
#         return ts
# 
#     def _transactions(self):
#         return _json(
#             self.requester.request(
#                 "GET",
#                 f"/characters/{self.character_id}/wallet/transactions",
#             )
#         )
# 
#     def _new_transaction_records(self, data, last):
#         return (
#             y for y in (
#                 {
#                     **x,
#                     "timestamp": parse_ts(x["ts"]),
#                     "character_id": self.character_id,
#                 }
#                 for x in data
#             )
#             if y["timestamp"] > last
#         )
# 
#     def record_transactions(self):
#         last = self._update_last("transactions")
#         data = self._transactions()
#         new = self._new_transaction_records(data, last)
#         result = self.executemany(
#             (
#                 "INSERT INTO transactions "
#                 "VALUES (:character_id, :item_id, :timestamp, :quantity, :unit_price, :location_id, :is_buy);"
#             ),
#             new,
#         )
#         return result
# 
#     def _assets(self):
#         return _json(
#             self.requester.request("GET", f"/characters/{self.character_id}/assets")
#         )
# 
#     def _aggregate_assets(self, data):
#         grouped = groupby(lambda x: x["type_id"], data)
#         return {k: sum(x["quantity"] for x in v) for (k, v) in grouped.items()}
# 
#     def record_assets(self):
#         data = self._assets()
#         found = self._aggregate_assets(data)
#         cur = self.execute(
#             "SELECT item_id,quantity FROM latest_assets WHERE character_id=:character_id;",
#             {"character_id": self.character_id},
#         )
#         latest = {x["item_id"]: x["quantity"] for x in cur.fetchall()}
#         updates = {
#             k: latest.get(k, 0)
#             for k in unique(itertools.chain(found.keys(), latest.keys()))
#             if latest.get(k, 0) != found.get(k, 0)
#         }
#         if updates:
#             now_ts = datetime.datetime.now().timestamp()
#             latest_records = (
#                 {
#                     "character_id": self.character_id,
#                     "item_id": k,
#                     "quantity": v,
#                 }
#                 for (k, v) in latest.items()
#             )
#             self.executemany(
#                 (
#                     "INSERT OR REPLACE INTO latest_assets "
#                     "VALUES (:character_id, :item_id, :quantity);"
#                 ),
#                 latest_records,
#             )
#         return self.executemany(
#             (
#                 "INSERT INTO assets "
#                 "VALUES (:character_id, :item_id, :timestamp, :quantity);"
#             ),
#             found,
#         )


# r0 = Requester("https://esi.evetech.net/latest/", EmptyToken())
# r = Requester("https://esi.evetech.net/latest/", eve_token("token.pkl"))
# universe = UniverseLookup(r0)
# items = ItemFactory(r0, "types.json")
# ua = UserAssets(r, "Mola Pavonis")
# entity = EntityFactory(items, universe)
# #db = WatcherDB("watcher.db")
# tm = TrackedMap("test.tm", value_type=int)
