import datetime
import itertools
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
