package io.github.sahilpatkar.ffp;

import io.github.sahilpatkar.ffp.avro.TripEvent;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * Week 1 job: Kafka (Avro via Schema Registry) to per-zone trip counts, printed to the TaskManager
 * log. It exists to prove the stack end to end; the real feature job replaces it in week 2.
 *
 * <p>Arguments, all optional: {@code --bootstrap}, {@code --schema-registry}, {@code --topic},
 * {@code --group}, {@code --out-of-orderness-s}, {@code --window-s}.
 */
public final class TripCountJob {

    private TripCountJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrap = params.get("bootstrap", "kafka:19092");
        String registry = params.get("schema-registry", "http://schema-registry:8081");
        String topic = params.get("topic", "trips.raw");
        String group = params.get("group", "trip-count-job");
        Duration outOfOrderness = Duration.ofSeconds(params.getLong("out-of-orderness-s", 10));
        Duration window = Duration.ofSeconds(params.getLong("window-s", 60));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);

        KafkaSource<TripEvent> source =
                KafkaSource.<TripEvent>builder()
                        .setBootstrapServers(bootstrap)
                        .setTopics(topic)
                        .setGroupId(group)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(
                                ConfluentRegistryAvroDeserializationSchema.forSpecific(
                                        TripEvent.class, registry))
                        .build();

        // Passing the strategy to fromSource makes watermarks per Kafka partition, so a slow
        // partition cannot make events from a fast one look late.
        DataStream<TripEvent> trips =
                env.fromSource(source, TripCountTopology.watermarks(outOfOrderness), "trips-kafka");

        SingleOutputStreamOperator<ZoneWindowCount> counts = TripCountTopology.build(trips, window);

        counts.print("count");
        counts.getSideOutput(TripCountTopology.LATE)
                .map(
                        trip ->
                                "trip=" + trip.getTripId()
                                        + " zone=" + trip.getPickupZoneId()
                                        + " dropoff_ts_ms=" + trip.getDropoffTsMs(),
                        Types.STRING)
                .name("format-late")
                .print("late");

        env.execute("trip-counts");
    }
}
