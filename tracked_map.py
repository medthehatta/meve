import datetime
import json
import os
import sqlite3
from textwrap import dedent

from cytoolz import unique

UNSET = object()


def dict_db(path):

    def dict_factory(cursor, row):
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    db = sqlite3.connect(path, isolation_level=None)
    db.row_factory = dict_factory

    return db


class TrackedMap:


    def __init__(
        self,
        path,
        default,
        encoder=str,
        decoder=str,
        missing_is_change=True,
    ):
        self.path = path
        self.encoder = encoder
        self.decoder = decoder
        self.default = self.encoder(default)
        self.db_path = os.path.join(self.path, "db")
        self.snap_path = os.path.join(self.path, "snap")
        self.snap_ts_path = os.path.join(self.path, "snaptime")
        self.missing_is_change = missing_is_change

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

    def _preprocess(self, data):
        snap = self.read_snap()
        data1 = {str(k): self.encoder(v) for (k, v) in data.items()}
        # If missing keys count as "removing" the entry, default the missing
        # entries in the data.
        if self.missing_is_change:
            missing = {k: self.default for k in snap if k not in data1}
            return {**missing, **data1}
        # Otherwise, if missing keys just mean we should keep the existing
        # value, hydrate the rest of the data with values from the snap.
        else:
            return {**snap, **data1}

    def write_snap(self, data, timestamp=None):
        timestamp = timestamp or datetime.datetime.now().timestamp()
        data = self._preprocess(data)

        if not os.path.exists(self.path):
            os.makedirs(self.path)
        if timestamp > self.last_snap_time():
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
        to_compare = self._preprocess(data)

        all_keys = to_compare.keys()
        return {
            k: to_compare[k] for k in all_keys
            if to_compare[k] != snap.get(k)
        }

    def to_row_seq(self, data, **meta):
        return [
            {**meta, "key": k, "value": v}
            for (k, v) in data.items()
        ]

    def record(self, data, timestamp):
        self.ensure_tables()
        ts = timestamp or datetime.datetime.now().timestamp()
        delta = self.differences(data)
        self.write_snap(data, timestamp=timestamp)
        with self.db:
            self.db.executemany(
                (
                    "INSERT INTO updates (timestamp, key, value) "
                    "VALUES (:ts, :key, :value);"
                ),
                self.to_row_seq(delta, ts=ts),
            )
            self.db.commit()

    def timeseries_for_key(self, key, days_back=60):
        self.ensure_tables()
        now = datetime.datetime.now().timestamp()
        before = now - days_back * (24*3600)
        with self.db:
            res = self.db.execute(
                (
                    "SELECT timestamp,key,value FROM updates WHERE "
                    "key=:key AND timestamp > :before "
                    "ORDER BY timestamp ASC;"
                ),
                {"key": key, "before": before},
            ).fetchall()
        return [
            {
                **record,
                "value": (
                    self.decoder(record["value"])
                    if record["value"] is not None else self.default
                ),
            }
            for record in res
        ]

    def value_and_time_data(self, when=None):
        when = when or datetime.datetime.now().timestamp()

        with self.db:
            res = self.db.execute(
                (
                    "SELECT timestamp,key,value FROM updates WHERE "
                    "timestamp <= :when ORDER BY timestamp ASC;"
                ),
                {"when": when},
            ).fetchall()

        result = {}
        time_data = {}

        for update in res:
            time_data[update["key"]] = {
                "timestamp": update["timestamp"],
                "value": self.decoder(update["value"]),
            }
            result[update["key"]] = self.decoder(update["value"])

        return (result, time_data)

    def value(self, when=None):
        return self.value_and_time_data(when)[0]
