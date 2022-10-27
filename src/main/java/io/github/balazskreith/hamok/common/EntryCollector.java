package io.github.balazskreith.hamok.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.*;
import java.util.stream.Collector;

import static java.util.stream.Collector.Characteristics.IDENTITY_FINISH;
import static java.util.stream.Collector.Characteristics.UNORDERED;

public class EntryCollector<K, V> implements Collector<Map.Entry<K, V>, Map<K, V>, Map<K, V>> {

    private static final Logger logger = LoggerFactory.getLogger(EntryCollector.class);

    public static<U, R> EntryCollector<U, R> create() {
        return create(logger);
    }

    public static<U, R> EntryCollector<U, R> create(Logger logger) {
        return create(logger, evet -> {});
    }

    public static<U, R> EntryCollector<U, R> create(Logger logger, Consumer<DetectedEntryCollision<U, R>> detectedEntryCollisionConsumer) {
        return new EntryCollector<U, R>(logger, detectedEntryCollisionConsumer, null);
    }

    public static<U, R> EntryCollector<U, R> create(Logger logger, Consumer<DetectedEntryCollision<U, R>> detectedEntryCollisionConsumer, String context) {
        return new EntryCollector<U, R>(logger, detectedEntryCollisionConsumer, context);
    }

    private final Consumer<DetectedEntryCollision<K, V>> detectedEntryCollisionConsumer;
    private final Logger actualLogger;
    private final String context;

    private EntryCollector(Logger logger, Consumer<DetectedEntryCollision<K, V>> detectedEntryCollisionConsumer, String context) {
        this.detectedEntryCollisionConsumer = detectedEntryCollisionConsumer;
        this.actualLogger = Utils.firstNonNull(logger, EntryCollector.logger);
        this.context =  Utils.firstNonNull(context, "No context is given");
    }

//    public EntryCollector<K, V> setContext(String value) {
//        return this;
//    }

    @Override
    public Supplier<Map<K, V>> supplier() {
        return HashMap<K, V>::new;
    }

    @Override
    public BiConsumer<Map<K, V>, Map.Entry<K, V>> accumulator() {
        return (result, entry) -> {
            var prevValue = result.put(entry.getKey(), entry.getValue());
            if (prevValue != null) {
                // detected collision
                logger.warn(
                    "Duplicated item tried to be merged for key {}. Given context: {}. Values: {}, {}",
                        entry.getKey(),
                        this.context,
                        prevValue,
                        entry.getValue()
                );
                detectedEntryCollisionConsumer.accept(new DetectedEntryCollision<>(
                        entry.getKey(),
                        prevValue,
                        entry.getValue()
                ));
            }
        };
    }

    @Override
    public BinaryOperator<Map<K, V>> combiner() {
        return (map1, map2) -> {
            var result = supplier().get();
            result.putAll(map1);
            for (var entry : map2.entrySet()) {
                var prevValue = result.put(entry.getKey(), entry.getValue());
                if (prevValue != null) {
                    logger.warn(
                            "Duplicated item tried to be merged for key {}. Given context: {}. Values: {}, {}",
                            entry.getKey(),
                            this.context,
                            prevValue,
                            entry.getValue()
                    );
                    detectedEntryCollisionConsumer.accept(new DetectedEntryCollision<>(
                            entry.getKey(),
                            prevValue,
                            entry.getValue()
                    ));
                }
            }
            return result;
        };
    }

    @Override
    public Function<Map<K, V>, Map<K, V>> finisher() {
        return Collections::unmodifiableMap;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return Set.of(
                UNORDERED,
                IDENTITY_FINISH
        );
    }
}
