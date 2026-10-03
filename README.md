# flink-feature-platform

A real-time feature platform for ride-hailing ETA prediction: streaming features computed in
Flink from Kafka, served from an online store, logged to Iceberg for point-in-time-correct
training sets, with an automated train/serve skew test.

**Status: week 1 of 6.** The stack runs end to end: a replayer pushes NYC taxi trips into Kafka
with controlled lateness, and a Flink job counts trips per pickup zone per event-time window.

```
replayer (Python) --Avro--> Kafka (trips.raw) --> Flink job --> window counts + late side output
                              ^ Schema Registry
```

## Quickstart

Needs Docker. Maven and Python are optional (the Makefile falls back to Docker for both).

```bash
make build     # compile, unit-test, and package the job jar
make up        # Kafka, Schema Registry, Flink (UI: http://localhost:8082)
make submit    # submit the job
make replay    # 50k synthetic trips at 60x speed
make logs      # watch window counts and late events
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
| `flink-jobs/` | Java 17 / Flink 2.2 job (Maven) |
| `docker-compose.yml` | Kafka 4 (KRaft), Schema Registry, Flink session cluster |

## Design notes

**Event time is dropoff time.** A trip only becomes observable when it ends, so features are
keyed on when the trip completed, not when it started.

**Lateness is injected, deterministic, and labelled.** Every event gets up to 5 s of jitter, and
2% get an extra exponential delay (mean 120 s, capped at 900 s). The schedule is a pure function
of the data and a seed, so a replay is reproducible. Each message carries an
`x-injected-delay-ms` Kafka header, which keeps test metadata out of the schema while letting
later weeks measure exactly how much deliberately late data was handled correctly.

**Late events are routed, not dropped.** The watermark trails the max event time by 10 s
(`--out-of-orderness-s`). Events behind it go to a side output, so the trade-off between
freshness and completeness is visible and tunable: `make late` counts them.

**The topology is separate from its sources and sinks** (`TripCountTopology`), so the same code
runs against Kafka in the cluster and against an in-memory stream in unit tests.

**Timestamps are replayed as recorded.** `--speedup` changes how fast event time passes on the
wall clock, not the timestamps themselves, so durations and speeds stay physically meaningful.

## Tests

```bash
make test
```

## Roadmap

| Week | Build |
|---|---|
| 1 | Compose stack, replayer, Avro schema, skeleton job (this) |
| 2 | Sliding-window features (5m, 15m, 1h), allowed lateness, operator test harness |
| 3 | Redis and Iceberg sinks, exactly-once, freshness metrics |
| 4 | Batch backfill from the same feature code, point-in-time join, model in MLflow |
| 5 | Serving API, feature logging, skew test as a CI gate |
| 6 | Load test, failure drills, design doc |
