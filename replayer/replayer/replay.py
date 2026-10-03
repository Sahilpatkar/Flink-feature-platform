"""CLI: replay trips into Kafka as Avro, paced against the wall clock.

    python -m replayer.replay --synthetic 50000 --speedup 60
    python -m replayer.replay --file data/yellow_tripdata_2024-01.parquet --speedup 600

Event timestamps are left exactly as they are in the source data. --speedup
only controls how fast event time passes on the wall clock: at 60, one hour of
trips is emitted per minute. Each message carries an `x-injected-delay-ms`
header, so a consumer can tell deliberately late events from on-time ones
without that test metadata leaking into the schema.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Callable, Iterator

from .lateness import LatenessConfig, Schedule, schedule
from .source import Trips, load_parquet, synthetic

log = logging.getLogger("replayer")

DEFAULT_SCHEMA = Path(__file__).resolve().parents[2] / "schemas" / "trip_event.avsc"
DELAY_HEADER = "x-injected-delay-ms"


class Pacer:
    """Maps emission times in the event-time domain onto the wall clock."""

    def __init__(
        self,
        speedup: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if speedup < 0:
            raise ValueError("speedup must be >= 0 (0 means as fast as possible)")
        self.speedup = speedup
        self._clock, self._sleep = clock, sleep
        self._wall_start: float | None = None
        self._emit_start_ms: int | None = None

    def wait_until(self, emit_ts_ms: int) -> None:
        if self.speedup == 0:
            return
        if self._wall_start is None:
            self._wall_start, self._emit_start_ms = self._clock(), emit_ts_ms
            return
        due = self._wall_start + (emit_ts_ms - self._emit_start_ms) / 1000.0 / self.speedup
        remaining = due - self._clock()
        if remaining > 0:
            self._sleep(remaining)


def planned_events(trips: Trips, plan: Schedule) -> Iterator[tuple[int, int, dict]]:
    """Yields (emit_ts_ms, injected_delay_ms, record) in emission order."""
    for i in plan.order:
        yield int(plan.emit_ts_ms[i]), int(plan.injected_delay_ms[i]), trips.record(int(i))


def run(args: argparse.Namespace) -> int:
    # Imported here so the pure logic above is testable without a Kafka client.
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    if args.synthetic:
        trips = synthetic(args.synthetic, seed=args.seed)
    else:
        trips = load_parquet(args.file, limit=args.limit)
    if len(trips) == 0:
        log.error("no trips to replay")
        return 1

    cfg = LatenessConfig(
        jitter_s=args.jitter_s,
        late_fraction=args.late_fraction,
        late_mean_s=args.late_mean_s,
        late_max_s=args.late_max_s,
        seed=args.seed,
    )
    plan = schedule(trips.dropoff_ts_ms, cfg)
    late = int((plan.injected_delay_ms > 0).sum())
    log.info("replaying %d trips, %d injected late (%.2f%%), speedup=%s",
             len(trips), late, 100.0 * late / len(trips), args.speedup or "max")

    serializer = AvroSerializer(
        SchemaRegistryClient({"url": args.schema_registry}),
        Path(args.schema).read_text(),
        # Pin the subject to "<topic>-value". Newer clients otherwise first ask the
        # registry for topic/subject associations, an endpoint not every registry has.
        conf={"subject.name.strategy": topic_subject_name_strategy},
    )
    ctx = SerializationContext(args.topic, MessageField.VALUE)
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "enable.idempotence": True,
        "linger.ms": 20,
        "compression.type": "lz4",
    })

    failed = 0

    def on_delivery(err, _msg) -> None:
        nonlocal failed
        if err is not None:
            failed += 1
            log.error("delivery failed: %s", err)

    pacer = Pacer(args.speedup)
    sent, last_log = 0, time.monotonic()
    try:
        for emit_ts_ms, delay_ms, rec in planned_events(trips, plan):
            pacer.wait_until(emit_ts_ms)
            payload = serializer(rec, ctx)
            while True:
                try:
                    producer.produce(
                        args.topic,
                        key=str(rec["pickup_zone_id"]).encode(),
                        value=payload,
                        headers=[(DELAY_HEADER, str(delay_ms).encode())],
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.5)
            producer.poll(0)
            sent += 1
            if time.monotonic() - last_log >= 5:
                log.info("sent %d / %d", sent, len(trips))
                last_log = time.monotonic()
    except KeyboardInterrupt:
        log.warning("interrupted after %d events", sent)
    finally:
        producer.flush(30)
    log.info("done: sent=%d failed=%d", sent, failed)
    return 1 if failed else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="replayer", description=__doc__.split("\n")[0])
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", help="TLC yellow taxi parquet file")
    src.add_argument("--synthetic", type=int, metavar="N", help="generate N fake trips instead")
    p.add_argument("--limit", type=int, help="replay only the first N trips of --file")
    p.add_argument("--speedup", type=float, default=60.0,
                   help="event-time seconds per wall-clock second; 0 = as fast as possible")
    p.add_argument("--jitter-s", type=float, default=5.0)
    p.add_argument("--late-fraction", type=float, default=0.02)
    p.add_argument("--late-mean-s", type=float, default=120.0)
    p.add_argument("--late-max-s", type=float, default=900.0)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--schema-registry", default="http://localhost:8081")
    p.add_argument("--topic", default="trips.raw")
    p.add_argument("--schema", default=str(DEFAULT_SCHEMA))
    return p.parse_args(argv)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(parse_args()))


if __name__ == "__main__":
    main()
