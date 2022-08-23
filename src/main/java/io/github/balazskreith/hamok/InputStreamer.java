package io.github.balazskreith.hamok;

import io.github.balazskreith.hamok.common.BatchCollector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface InputStreamer<K, V> {

    Stream<Set<K>> streamKeys(Set<K> set);
    Stream<Map<K, V>> streamEntries(Map<K, V> entries);

    static<U, R> InputStreamer<U, R> createSelfStreamer() {
        return new InputStreamer<U, R>() {
            @Override
            public Stream<Set<U>> streamKeys(Set<U> keys) {
                return Stream.of(keys);
            }

            @Override
            public Stream<Map<U, R>> streamEntries(Map<U, R> entries) {
                return Stream.of(entries);
            }
        };
    }

    static<U, R> InputStreamer<U, R> create(int maxMessageKeys, int maxMessageValues) {
        if (maxMessageKeys < 1 && maxMessageValues < 1) {
            return createSelfStreamer();
        }
        return new InputStreamer<U, R>() {
            @Override
            public Stream<Set<U>> streamKeys(Set<U> keys) {
                if (maxMessageKeys < 1) {
                    return Stream.of(keys);
                }
                var streamBuilder = Stream.<Set<U>>builder();
                var collector = BatchCollector.<U, Set<U>>builder().withBatchSize(maxMessageKeys)
                        .withEmptyCollectionSupplier(Collections::emptySet)
                        .withMutableCollectionSupplier(HashSet::new)
                        .withConsumer(batch -> {
                            streamBuilder.add(batch);
                        }).build();
                keys.stream().collect(collector);
                return streamBuilder.build();
            }

            @Override
            public Stream<Map<U, R>> streamEntries(Map<U, R> entries) {
                var batchSize = Math.min(maxMessageKeys, maxMessageValues);
                if (batchSize < 1) {
                    batchSize = Math.max(maxMessageKeys, maxMessageValues);
                    if (batchSize < 1) {
                        return Stream.of(entries);
                    }
                }
                var streamBuilder = Stream.<Map<U, R>>builder();
                var collector = BatchCollector.<Map.Entry<U, R>, Set<Map.Entry<U, R>>>builder().withBatchSize(batchSize)
                        .withEmptyCollectionSupplier(Collections::emptySet)
                        .withMutableCollectionSupplier(HashSet::new)
                        .withConsumer(batch -> {
                            var entryBatch = batch.stream().collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue
                            ));
                            streamBuilder.add(entryBatch);
                        }).build();
                entries.entrySet().stream().collect(collector);
                return streamBuilder.build();
            }
        };
    }
}
