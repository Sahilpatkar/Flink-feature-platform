# flink-feature-platform

A real-time feature platform for ride-hailing ETA prediction: streaming features computed in
Flink from Kafka, served from an online store, logged to Iceberg for point-in-time-correct
training sets, with an automated train/serve skew test.

**Status: week 2 of 6.** The feature job runs end to end: a replayer pushes NYC taxi trips into
Kafka with controlled lateness, and Flink turns them into per-zone features over 5 minute,
15 minute, and 1 hour windows, one row per zone per minute, corrected when late data arrives.

```
replayer --Avro--> Kafka --> split by zone --> 1-minute panes --> rollup --> feature rows
                     ^                              |
              Schema Registry                       +--> too late to count (side output)
```

| Feature (per zone, per window) | Definition |
|---|---|
| `tripCount` | trips that started in the zone |
| `dropoffCount` | trips that ended in the zone |
| `avgSpeedMph` | total distance / total duration of trips from the zone |
| `avgFare` | mean fare of trips from the zone |
| `pickupDropoffRatio` | `tripCount / dropoffCount`, a demand-imbalance signal |

## Quickstart

Needs Docker. Maven and Python are optional (the Makefile falls back to Docker for both).

```bash
make build     # compile, unit-test, and package the job jar
make up        # Kafka, Schema Registry, Flink (UI: http://localhost:8082)
make submit    # submit the feature job
make replay    # 50k synthetic trips at 60x speed
make logs      # watch feature rows and late trips
make late      # trips that arrived too late to count
make corrections   # rows re-emitted because late data changed them
make down
```

Real data instead of synthetic: `make replay-tlc` downloads one month of
[NYC TLC yellow taxi trips](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
(about 50 MB, 3M trips) and replays it. `SPEEDUP=600 make replay-tlc` goes faster.

## Layout

| Path | What |
|---|---|
| `schemas/trip_event.avsc` | The event contract, shared by the replayer and the Flink job |
| `replayer/` | Python CLI that replays trips with injected lateness and disorder |
| `flink-jobs/` | Java 17 / Flink 2.2 jobs (Maven): `FeatureJob`, plus the week 1 `TripCountJob` smoke test |
| `docker-compose.yml` | Kafka 4 (KRaft), Schema Registry, Flink session cluster |

## Design notes

**Event time is dropoff time.** A trip only becomes observable when it ends, so features are
keyed on when the trip completed, not when it started.

**Lateness is injected, deterministic, and labelled.** Every event gets up to 5 s of jitter, and
2% get an extra exponential delay (mean 120 s, capped at 900 s). The schedule is a pure function
of the data and a seed, so a replay is reproducible. Each message carries an
`x-injected-delay-ms` Kafka header, which keeps test metadata out of the schema while letting
later weeks measure exactly how much deliberately late data was handled correctly.

**Windows are built from one-minute panes, not Flink sliding windows.** A one-hour window
sliding by a minute would put every event into 60 windows, and three sizes would triple that.
Here an event updates a single pane, and each window is a sum over at most 60 panes
(`RollupFunction`). Panes hold only counts and sums, so they combine into any window size.

**Late data corrects rows instead of being lost.** A row's identity is `(zoneId, asOfMs)`. The
watermark trails the max event time by 10 s (`--out-of-orderness-s`). An event later than that
but within the allowed lateness (`--allowed-lateness-s`, default 15 min) updates its pane, and
every already-emitted row containing that pane is emitted again with `correction=true`; a store
keyed on the row identity converges to the right value. Only events later still go to a side
output and are not counted.

| Run (50k synthetic trips, 2% injected late, 60x speed) | Trips not counted |
|---|---|
| Week 1: no allowed lateness | 627 (1.25%) |
| Week 2: 15 min allowed lateness | 0, with 3,161 feature rows corrected |

Lateness results depend on replay speed: Flink emits watermarks every 200 ms of wall time,
which at 60x is 12 s of event time.

**Quiet zones still get rows.** A zone with no trips in a minute produces no pane, but its
features change as old panes age out. A per-zone timer keeps emitting until the zone's windows
are empty, then emits one all-zero row and stops, so a store never serves stale non-zero values.

**The topology is separate from its sources and sinks** (`TripCountTopology`), so the same code
runs against Kafka in the cluster and against an in-memory stream in unit tests.

**Timestamps are replayed as recorded.** `--speedup` changes how fast event time passes on the
wall clock, not the timestamps themselves, so durations and speeds stay physically meaningful.

## Tests

```bash
make test
```

The pipeline tests run the real topology in a mini cluster with a watermark after every event,
so lateness is deterministic. `streamingOutputMatchesBatchRecomputation` is the central one: a
few thousand trips arrive out of order, some late enough to be corrected and some late enough to
be dropped, and the last row emitted for every `(zone, as-of)` must equal a direct batch
recomputation over the same events.

## Roadmap

| Week | Build |
|---|---|
| 1 | Compose stack, replayer, Avro schema, skeleton job (done) |
| 2 | Window features (5m, 15m, 1h), allowed lateness with corrections, deterministic lateness tests (this) |
| 3 | Redis and Iceberg sinks, exactly-once, freshness metrics |
| 4 | Batch backfill from the same feature code, point-in-time join, model in MLflow |
| 5 | Serving API, feature logging, skew test as a CI gate |
| 6 | Load test, failure drills, design doc |
