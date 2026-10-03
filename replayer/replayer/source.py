"""Loads trips as plain numpy columns, from a TLC parquet file or synthetically."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import numpy as np

MAX_TRIP_MS = 6 * 60 * 60 * 1000
# TLC files contain a few rows with garbage dates (2002, 2098...). Rows further
# than this from the file's median dropoff are dropped.
MAX_DISTANCE_FROM_MEDIAN_MS = 45 * 24 * 60 * 60 * 1000


@dataclass(frozen=True)
class Trips:
    vendor_id: np.ndarray
    pickup_ts_ms: np.ndarray
    dropoff_ts_ms: np.ndarray
    pickup_zone_id: np.ndarray
    dropoff_zone_id: np.ndarray
    # NaN means unknown.
    passenger_count: np.ndarray
    trip_distance_miles: np.ndarray
    fare_amount: np.ndarray
    total_amount: np.ndarray

    def __len__(self) -> int:
        return int(self.dropoff_ts_ms.shape[0])

    def record(self, i: int) -> dict:
        """Row i as a dict matching schemas/trip_event.avsc."""
        pc = self.passenger_count[i]
        rec = {
            "vendor_id": int(self.vendor_id[i]),
            "pickup_ts_ms": int(self.pickup_ts_ms[i]),
            "dropoff_ts_ms": int(self.dropoff_ts_ms[i]),
            "pickup_zone_id": int(self.pickup_zone_id[i]),
            "dropoff_zone_id": int(self.dropoff_zone_id[i]),
            "passenger_count": None if np.isnan(pc) else int(pc),
            "trip_distance_miles": float(self.trip_distance_miles[i]),
            "fare_amount": float(self.fare_amount[i]),
            "total_amount": float(self.total_amount[i]),
        }
        rec["trip_id"] = trip_id(rec, i)
        return rec


def trip_id(rec: dict, row: int) -> str:
    """Stable id: the same source row always gets the same id across replays."""
    raw = "|".join(
        str(rec[k])
        for k in (
            "vendor_id",
            "pickup_ts_ms",
            "dropoff_ts_ms",
            "pickup_zone_id",
            "dropoff_zone_id",
            "trip_distance_miles",
            "total_amount",
        )
    )
    return hashlib.sha1(f"{row}|{raw}".encode()).hexdigest()[:16]


def load_parquet(path: str | Path, limit: int | None = None) -> Trips:
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "total_amount",
    ]
    table = pq.read_table(path, columns=cols)

    def ts_ms(name: str) -> np.ndarray:
        # TLC timestamps are naive New York local time; they are treated as UTC.
        col = pc.cast(table[name], "timestamp[ms]", safe=False)
        return pc.cast(col, "int64").to_numpy(zero_copy_only=False).astype(np.float64)

    def num(name: str) -> np.ndarray:
        return pc.cast(table[name], "float64").to_numpy(zero_copy_only=False)

    pickup, dropoff = ts_ms("tpep_pickup_datetime"), ts_ms("tpep_dropoff_datetime")
    vendor, pu, do = num("VendorID"), num("PULocationID"), num("DOLocationID")
    dist, fare, total = num("trip_distance"), num("fare_amount"), num("total_amount")

    required = np.stack([pickup, dropoff, vendor, pu, do, dist, fare, total])
    keep = ~np.isnan(required).any(axis=0)
    keep &= (dropoff > pickup) & (dropoff - pickup <= MAX_TRIP_MS) & (dist >= 0)
    keep &= np.abs(dropoff - np.nanmedian(dropoff)) <= MAX_DISTANCE_FROM_MEDIAN_MS

    trips = Trips(
        vendor_id=vendor[keep].astype(np.int64),
        pickup_ts_ms=pickup[keep].astype(np.int64),
        dropoff_ts_ms=dropoff[keep].astype(np.int64),
        pickup_zone_id=pu[keep].astype(np.int64),
        dropoff_zone_id=do[keep].astype(np.int64),
        passenger_count=num("passenger_count")[keep],
        trip_distance_miles=dist[keep],
        fare_amount=fare[keep],
        total_amount=total[keep],
    )
    return head(sort_by_event_time(trips), limit)


def synthetic(n: int, seed: int = 7, start_ts_ms: int = 1_704_067_200_000) -> Trips:
    """Fake trips for smoke tests and CI, when no TLC file has been downloaded."""
    rng = np.random.default_rng(seed)
    # About 20 dropoffs per second, like a busy hour in the real data.
    dropoff = start_ts_ms + np.cumsum(rng.exponential(50.0, size=n)).astype(np.int64)
    duration_ms = rng.integers(3 * 60_000, 45 * 60_000, size=n)
    dist = np.round(rng.gamma(2.0, 1.5, size=n), 2)
    fare = np.round(3.0 + 2.5 * dist + rng.normal(0, 1, size=n).clip(-2, 2), 2)
    pc_ = rng.integers(1, 5, size=n).astype(np.float64)
    pc_[rng.random(n) < 0.03] = np.nan
    return Trips(
        vendor_id=rng.integers(1, 3, size=n),
        pickup_ts_ms=dropoff - duration_ms,
        dropoff_ts_ms=dropoff,
        pickup_zone_id=rng.integers(1, 264, size=n),
        dropoff_zone_id=rng.integers(1, 264, size=n),
        passenger_count=pc_,
        trip_distance_miles=dist,
        fare_amount=fare,
        total_amount=np.round(fare * 1.2, 2),
    )


def sort_by_event_time(t: Trips) -> Trips:
    order = np.argsort(t.dropoff_ts_ms, kind="stable")
    return Trips(**{k: v[order] for k, v in t.__dict__.items()})


def head(t: Trips, limit: int | None) -> Trips:
    if limit is None or limit >= len(t):
        return t
    return Trips(**{k: v[:limit] for k, v in t.__dict__.items()})
