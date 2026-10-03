import io
import json
from pathlib import Path

import fastavro
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from replayer.lateness import LatenessConfig, schedule
from replayer.replay import Pacer, parse_args, planned_events
from replayer.source import load_parquet, synthetic

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[2] / "schemas" / "trip_event.avsc").read_text()
)


def test_schedule_is_deterministic_for_a_seed():
    ts = synthetic(5_000).dropoff_ts_ms
    a, b = schedule(ts, LatenessConfig(seed=1)), schedule(ts, LatenessConfig(seed=1))
    c = schedule(ts, LatenessConfig(seed=2))
    assert np.array_equal(a.order, b.order)
    assert not np.array_equal(a.emit_ts_ms, c.emit_ts_ms)


def test_emission_order_is_sorted_and_never_before_event_time():
    ts = synthetic(5_000).dropoff_ts_ms
    s = schedule(ts, LatenessConfig())
    assert np.all(np.diff(s.emit_ts_ms[s.order]) >= 0)
    assert np.all(s.emit_ts_ms >= ts)
    assert sorted(s.order.tolist()) == list(range(len(ts)))


def test_late_fraction_and_cap_are_respected():
    ts = synthetic(50_000).dropoff_ts_ms
    cfg = LatenessConfig(late_fraction=0.1, late_mean_s=300, late_max_s=600)
    s = schedule(ts, cfg)
    frac = (s.injected_delay_ms > 0).mean()
    assert 0.09 < frac < 0.11
    assert s.injected_delay_ms.max() <= 600_000


def test_no_disorder_when_lateness_is_disabled():
    ts = synthetic(2_000).dropoff_ts_ms
    s = schedule(ts, LatenessConfig(jitter_s=0, late_fraction=0))
    assert np.array_equal(s.order, np.arange(len(ts)))
    assert np.array_equal(s.emit_ts_ms, ts)


def test_late_events_arrive_out_of_event_time_order():
    trips = synthetic(5_000)
    plan = schedule(trips.dropoff_ts_ms, LatenessConfig(late_fraction=0.05))
    event_times = [rec["dropoff_ts_ms"] for _, _, rec in planned_events(trips, plan)]
    assert any(b < a for a, b in zip(event_times, event_times[1:]))


def test_invalid_config_is_rejected():
    with pytest.raises(ValueError):
        LatenessConfig(late_fraction=1.5)
    with pytest.raises(ValueError):
        LatenessConfig(jitter_s=-1)


def test_records_conform_to_the_avro_schema():
    trips = synthetic(500)
    parsed = fastavro.parse_schema(SCHEMA)
    plan = schedule(trips.dropoff_ts_ms, LatenessConfig())
    records = [rec for _, _, rec in planned_events(trips, plan)]
    assert any(r["passenger_count"] is None for r in records)
    buf = io.BytesIO()
    fastavro.writer(buf, parsed, records)  # raises if any record does not match
    assert len({r["trip_id"] for r in records}) == len(records)


def test_trip_ids_are_stable_across_replays():
    a, b = synthetic(100), synthetic(100)
    assert [a.record(i)["trip_id"] for i in range(100)] == [
        b.record(i)["trip_id"] for i in range(100)
    ]


def test_pacer_sleeps_proportionally_to_speedup():
    now, sleeps = [100.0], []

    def sleep(s):
        sleeps.append(s)
        now[0] += s

    pacer = Pacer(speedup=60, clock=lambda: now[0], sleep=sleep)
    pacer.wait_until(0)
    pacer.wait_until(60_000)   # one event-time minute later = 1 wall second
    pacer.wait_until(60_000)   # already due
    pacer.wait_until(180_000)  # two more event-time minutes = 2 wall seconds
    assert sleeps == pytest.approx([1.0, 2.0])


def test_pacer_never_sleeps_at_speedup_zero():
    sleeps = []
    pacer = Pacer(speedup=0, sleep=sleeps.append)
    pacer.wait_until(0)
    pacer.wait_until(10_000_000)
    assert sleeps == []


def test_load_parquet_drops_bad_rows_and_sorts(tmp_path):
    base = np.datetime64("2024-01-15T12:00:00", "us")
    minute = np.timedelta64(60_000_000, "us")
    pickup = [base + 2 * minute, base, base + minute, base, np.datetime64("2002-01-01T00:00:00", "us")]
    dropoff = [
        base + 12 * minute,                            # ok, second by dropoff
        base + 5 * minute,                             # ok, first by dropoff
        base,                                          # dropoff before pickup
        base + 600 * minute,                           # 10 hour trip
        np.datetime64("2002-01-01T00:10:00", "us"),  # garbage date
    ]
    table = pa.table({
        "VendorID": pa.array([1, 2, 1, 2, 1], pa.int32()),
        "tpep_pickup_datetime": pa.array(pickup, pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(dropoff, pa.timestamp("us")),
        "passenger_count": pa.array([1.0, None, 2.0, 1.0, 1.0], pa.float64()),
        "trip_distance": pa.array([2.5, 1.0, 1.0, 1.0, 1.0]),
        "PULocationID": pa.array([10, 20, 30, 40, 50], pa.int32()),
        "DOLocationID": pa.array([11, 21, 31, 41, 51], pa.int32()),
        "fare_amount": pa.array([12.0, 7.0, 5.0, 5.0, 5.0]),
        "total_amount": pa.array([15.0, 9.0, 6.0, 6.0, 6.0]),
    })
    path = tmp_path / "trips.parquet"
    pq.write_table(table, path)

    trips = load_parquet(path)
    assert trips.pickup_zone_id.tolist() == [20, 10]
    first = trips.record(0)
    assert first["passenger_count"] is None
    assert first["dropoff_ts_ms"] - first["pickup_ts_ms"] == 5 * 60_000
    assert len(load_parquet(path, limit=1)) == 1


def test_cli_requires_a_source():
    with pytest.raises(SystemExit):
        parse_args([])
    assert parse_args(["--synthetic", "10"]).topic == "trips.raw"
