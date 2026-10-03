package io.github.sahilpatkar.ffp;

import io.github.sahilpatkar.ffp.avro.TripEvent;
import io.github.sahilpatkar.ffp.features.FeatureTopology;
import java.time.Duration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

/**
 * The feature job: Kafka trips to per-zone features over 5 minute, 15 minute, and 1 hour windows,
 * one row per zone per minute. Until the sinks land in week 3 the rows are printed.
 *
 * <p>Arguments, all optional: {@code --bootstrap}, {@code --schema-registry}, {@code --topic},
 * {@code --group}, {@code --out-of-orderness-s}, {@code --allowed-lateness-s}.
 */
public final class FeatureJob {

    private FeatureJob() {}

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        String bootstrap = params.get("bootstrap", "kafka:19092");
        String registry = params.get("schema-registry", "http://schema-registry:8081");
        String topic = params.get("topic", "trips.raw");
        String group = params.get("group", "feature-job");
        Duration outOfOrderness = Duration.ofSeconds(params.getLong("out-of-orderness-s", 10));
        Duration allowedLateness = Duration.ofSeconds(params.getLong("allowed-lateness-s", 900));

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

        DataStream<TripEvent> trips =
                env.fromSource(source, TripCountTopology.watermarks(outOfOrderness), "trips-kafka")
                        .uid("trips-kafka");

        FeatureTopology.Result result = FeatureTopology.build(trips, allowedLateness);

        result.features.print("feature");
        // Each late trip appears once per zone it touches; keep the origin side so the count
        // is in trips.
        result.tooLate.filter(obs -> obs.origin).name("late-trips").print("late");

        env.execute("zone-features");
    }
}
