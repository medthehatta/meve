import datetime
import itertools
import time

from api_access import authed_requester
from tracked_map import TrackedMap
from user_data import UserAssets


class AssetWatcher:

    def __init__(
        self,
        path,
        user_data: UserAssets,
        poll_seconds=100,
    ):
        self.tracker = TrackedMap(
            path,
            default=0,
            encoder=str,
            decoder=int,
            missing_is_change=True,
        )
        self.ua = user_data
        self.poll_seconds = poll_seconds
        self.last_snap_time = self.tracker.last_snap_time()

    def watch(self, poll_seconds=None):
        poll_seconds = poll_seconds or self.poll_seconds
        while True:
            txs = self.ua.transactions()
            new = list(
                itertools.takewhile(
                    lambda tx: self.more_recent(tx["date"]),
                    txs,
                )
            )
            if new:
                self._update(new)
            time.sleep(poll_seconds)

    def more_recent(self, date):
        ts = datetime.datetime.strptime(
            date,
            "%Y-%m-%dT%H:%M:%SZ",
        ).timestamp()
        return (ts > self.last_snap_time)

    def _update(self, new_transactions):
        totals = self.ua.total_quantities()
        print(
            f"{time.time()} Detected {len(new_transactions)} new records "
            f"since {self.last_snap_time}, updating..."
        )
        for tx in new_transactions:
            print(tx)
            timestamp = datetime.datetime.strptime(
                tx["date"],
                "%Y-%m-%dT%H:%M:%SZ",
            ).timestamp()
            if tx["is_buy"]:
                pre_tx = totals.get(tx["type_id"], 0) - tx["quantity"]
            else:
                pre_tx = totals.get(tx["type_id"], 0) + tx["quantity"]
            totals[tx["type_id"]] = max(0, pre_tx)
            self.tracker.record(totals, timestamp=timestamp)
        self.last_snap_time = self.tracker.last_snap_time()
