package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.BatchCollector;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class InputStreamer<K, V> {
    private final int maxKeys;
    private final int maxValues;
    private final int maxEntries;

    public InputStreamer(int maxKeys, int maxValues) {
        this.maxKeys = maxKeys;
        this.maxValues = maxValues;
        if (this.maxKeys < 1 && this.maxValues < 1) {
            this.maxEntries = 0;
        } else if (this.maxKeys < 1) {
            this.maxEntries = this.maxValues;
        } else if (this.maxValues < 1) {
            this.maxEntries = this.maxKeys;
        } else {
            this.maxEntries = Math.min(this.maxKeys, this.maxValues);
        }
    }

    public Stream<Map<K, V>> streamEntries(Map<K, V> source) {
        if (source == null) {
            return Stream.empty();
        }
        if (this.maxEntries < 1 || source.size() <= this.maxEntries) {
            return Stream.of(source);
        }
        var result = Stream.<Map<K, V>>builder();
        var collector = BatchCollector.<Map.Entry<K, V>, Set<Map.Entry<K, V>>>builder()
                .withBatchSize(this.maxEntries)
                .withEmptyCollectionSupplier(Collections::emptySet)
                .withMutableCollectionSupplier(HashSet::new)
                .withConsumer(batch -> {
                    var entryBatch = batch.stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
                    result.add(entryBatch);
                }).build();
        source.entrySet().stream().collect(collector);
        return result.build();
    }

    public Stream<Set<K>> streamKeys(Set<K> source) {
        if (source == null) {
            return Stream.empty();
        }
        if (this.maxKeys < 1 || source.size() <= this.maxKeys) {
            return Stream.of(source);
        }
        var result = Stream.<Set<K>>builder();
        var collector = BatchCollector.<K, Set<K>>builder()
                .withBatchSize(this.maxKeys)
                .withEmptyCollectionSupplier(Collections::emptySet)
                .withMutableCollectionSupplier(HashSet::new)
                .withConsumer(batch -> {
                    result.add(batch);
                }).build();
        source.stream().collect(collector);
        return result.build();
    }

}
