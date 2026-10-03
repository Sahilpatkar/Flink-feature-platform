package io.github.sahilpatkar.ffp.testing;

import io.github.sahilpatkar.ffp.avro.TripEvent;
import io.github.sahilpatkar.ffp.features.FeatureRow;
import io.github.sahilpatkar.ffp.features.FeatureTopology;
import io.github.sahilpatkar.ffp.features.ZoneObservation;
import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Test fixtures: building trips and running the feature pipeline over a fixed arrival order. */
public final class Trips {
    public static final long MINUTE = 60_000L;
    /** 2024-01-01T00:00:00Z: aligned to the minute and to the hour. */
    public static final long T0 = 1_704_067_200_000L;

    private Trips() {}

    public static TripEvent trip(
            String id,
            int pickupZone,
            int dropoffZone,
            long dropoffTsMs,
            long durationMs,
            double distanceMiles,
            double fare) {
        return TripEvent.newBuilder()
                .setTripId(id)
                .setVendorId(1)
                .setPickupTsMs(dropoffTsMs - durationMs)
                .setDropoffTsMs(dropoffTsMs)
                .setPickupZoneId(pickupZone)
                .setDropoffZoneId(dropoffZone)
                .setPassengerCount(1)
                .setTripDistanceMiles(distanceMiles)
                .setFareAmount(fare)
                .setTotalAmount(fare)
                .build();
    }

    /** A plain trip from {@code pickupZone} to zone 999: 10 minutes, 2 miles, 10 dollars. */
    public static TripEvent trip(String id, int pickupZone, long dropoffTsMs) {
        return trip(id, pickupZone, 999, dropoffTsMs, 10 * MINUTE, 2.0, 10.0);
    }

    private static TypeInformation<TripEvent> tripType() {
        return new AvroTypeInfo<>(TripEvent.class);
    }

    /** Everything the pipeline emitted, in emission order. */
    public static final class Output {
        public final List<FeatureRow> rows;
        public final List<ZoneObservation> tooLate;

        Output(List<FeatureRow> rows, List<ZoneObservation> tooLate) {
            this.rows = rows;
            this.tooLate = tooLate;
        }
    }

    /**
     * Runs the pipeline over {@code arrivals}, in exactly that order.
     *
     * <p>Lateness is deterministic here: parallelism is 1 and a watermark of {@code max event
     * time - outOfOrderness - 1} follows every single event, rather than being emitted on a
     * wall-clock timer as in production. Whether an event is late therefore depends only on the
     * events before it.
     */
    public static Output run(List<TripEvent> arrivals, Duration outOfOrderness, Duration allowedLateness)
            throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<TripEvent> trips =
                env.fromData(arrivals, tripType())
                        .assignTimestampsAndWatermarks(perEventWatermarks(outOfOrderness.toMillis()));

        FeatureTopology.Result result = FeatureTopology.build(trips, allowedLateness);
        CollectSink<FeatureRow> rows = new CollectSink<>();
        CollectSink<ZoneObservation> tooLate = new CollectSink<>();
        result.features.sinkTo(rows);
        result.tooLate.sinkTo(tooLate);

        env.execute("test");
        return new Output(rows.results(), tooLate.results());
    }

    static WatermarkStrategy<TripEvent> perEventWatermarks(long outOfOrdernessMs) {
        return WatermarkStrategy.<TripEvent>forGenerator(ctx -> new PerEvent(outOfOrdernessMs))
                .withTimestampAssigner((trip, recordTs) -> trip.getDropoffTsMs());
    }

    private static final class PerEvent implements WatermarkGenerator<TripEvent> {
        private final long outOfOrdernessMs;
        private long maxTs = Long.MIN_VALUE;

        PerEvent(long outOfOrdernessMs) {
            this.outOfOrdernessMs = outOfOrdernessMs;
        }

        @Override
        public void onEvent(TripEvent trip, long eventTs, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTs);
            output.emitWatermark(new Watermark(maxTs - outOfOrdernessMs - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
