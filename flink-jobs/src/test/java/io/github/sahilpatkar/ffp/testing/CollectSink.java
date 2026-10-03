package io.github.sahilpatkar.ffp.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Collects a stream into memory, in arrival order. Works because the test mini cluster runs in
 * the test's own JVM; each sink writes to its own list, found again by id after serialization.
 */
public final class CollectSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;
    private static final Map<String, List<Object>> RESULTS = new ConcurrentHashMap<>();

    private final String id = UUID.randomUUID().toString();

    public CollectSink() {
        RESULTS.put(id, new CopyOnWriteArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public List<T> results() {
        return new ArrayList<>((List<T>) RESULTS.get(id));
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        final List<Object> target = RESULTS.get(id);
        return new SinkWriter<T>() {
            @Override
            public void write(T element, Context ctx) {
                target.add(element);
            }

            @Override
            public void flush(boolean endOfInput) {}

            @Override
            public void close() {}
        };
    }
}
