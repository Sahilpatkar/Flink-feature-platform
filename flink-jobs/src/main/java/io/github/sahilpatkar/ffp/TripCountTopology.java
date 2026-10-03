package io.github.sahilpatkar.ffp;

import io.github.sahilpatkar.ffp.avro.TripEvent;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * The pipeline, kept apart from sources and sinks so tests can run it on an in-memory stream.
 *
 * <p>Week 1 computes one thing: trips per pickup zone per tumbling event-time window. Events that
 * arrive after the watermark has passed their window are not dropped silently; they go to the
 * {@link #LATE} side output.
 */
public final class TripCountTopology {

    public static final OutputTag<TripEvent> LATE = new OutputTag<TripEvent>("late-trips") {};

    private TripCountTopology() {}

    /** Event time is the dropoff time; the watermark trails the max seen by {@code maxOutOfOrderness}. */
    public static WatermarkStrategy<TripEvent> watermarks(Duration maxOutOfOrderness) {
        return WatermarkStrategy.<TripEvent>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner((trip, recordTs) -> trip.getDropoffTsMs())
                // Without this, one empty Kafka partition would hold the watermark back forever.
                .withIdleness(Duration.ofSeconds(30));
    }

    /** Expects {@code trips} to already carry timestamps and watermarks. */
    public static SingleOutputStreamOperator<ZoneWindowCount> build(
            DataStream<TripEvent> trips, Duration window) {
        return trips.keyBy(trip -> trip.getPickupZoneId(), Types.INT)
                .window(TumblingEventTimeWindows.of(window))
                .sideOutputLateData(LATE)
                .aggregate(new CountTrips(), new AttachWindow())
                .name("trip-count-per-zone");
    }

    /** Incremental count: state per key and window is a single long, not the buffered events. */
    static final class CountTrips implements AggregateFunction<TripEvent, Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(TripEvent trip, Long count) {
            return count + 1;
        }

        @Override
        public Long getResult(Long count) {
            return count;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    static final class AttachWindow
            extends ProcessWindowFunction<Long, ZoneWindowCount, Integer, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(
                Integer zoneId, Context ctx, Iterable<Long> counts, Collector<ZoneWindowCount> out) {
            TimeWindow w = ctx.window();
            out.collect(
                    new ZoneWindowCount(zoneId, w.getStart(), w.getEnd(), counts.iterator().next()));
        }
    }
}
