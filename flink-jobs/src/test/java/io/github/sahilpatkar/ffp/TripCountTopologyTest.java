package io.github.sahilpatkar.ffp;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.sahilpatkar.ffp.avro.TripEvent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

class TripCountTopologyTest {

    private static final long MINUTE = 60_000L;
    private static final long T0 = 1_704_067_200_000L; // 2024-01-01T00:00:00Z, a window boundary

    @Test
    void countsTripsPerZonePerEventTimeWindow() throws Exception {
        List<ZoneWindowCount> results =
                run(
                        trip("a", 7, T0 + 1_000),
                        trip("b", 7, T0 + 59_000),
                        trip("c", 7, T0 + MINUTE + 5_000),
                        trip("d", 42, T0 + 30_000));

        assertThat(results)
                .containsExactlyInAnyOrder(
                        new ZoneWindowCount(7, T0, T0 + MINUTE, 2),
                        new ZoneWindowCount(7, T0 + MINUTE, T0 + 2 * MINUTE, 1),
                        new ZoneWindowCount(42, T0, T0 + MINUTE, 1));
    }

    @Test
    void outOfOrderEventsLandInTheWindowOfTheirEventTime() throws Exception {
        // Arrival order differs from event-time order; the result must not.
        List<ZoneWindowCount> results =
                run(
                        trip("late-arrival-window-2", 7, T0 + MINUTE + 1_000),
                        trip("window-1", 7, T0 + 2_000),
                        trip("window-2", 7, T0 + MINUTE + 9_000));

        assertThat(results)
                .containsExactlyInAnyOrder(
                        new ZoneWindowCount(7, T0, T0 + MINUTE, 1),
                        new ZoneWindowCount(7, T0 + MINUTE, T0 + 2 * MINUTE, 2));
    }

    private static List<ZoneWindowCount> run(TripEvent... trips) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<TripEvent> stream =
                env.fromData(new AvroTypeInfo<>(TripEvent.class), trips)
                        .assignTimestampsAndWatermarks(
                                TripCountTopology.watermarks(Duration.ofSeconds(10)));

        List<ZoneWindowCount> out = new ArrayList<>();
        try (CloseableIterator<ZoneWindowCount> it =
                TripCountTopology.build(stream, Duration.ofMinutes(1)).executeAndCollect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private static TripEvent trip(String id, int pickupZone, long dropoffTsMs) {
        return TripEvent.newBuilder()
                .setTripId(id)
                .setVendorId(1)
                .setPickupTsMs(dropoffTsMs - 10 * MINUTE)
                .setDropoffTsMs(dropoffTsMs)
                .setPickupZoneId(pickupZone)
                .setDropoffZoneId(1)
                .setPassengerCount(1)
                .setTripDistanceMiles(2.5)
                .setFareAmount(12.0)
                .setTotalAmount(15.0)
                .build();
    }
}
