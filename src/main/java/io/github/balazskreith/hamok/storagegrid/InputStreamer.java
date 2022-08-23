package io.github.balazskreith.hamok.storagegrid;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
        var sourceIterator = source.entrySet().iterator();
        Iterator<Map<K, V>> resultIterator = new Iterator<Map<K, V>>() {
            @Override
            public boolean hasNext() {
                return sourceIterator.hasNext();
            }

            @Override
            public Map<K, V> next() {
                var batch = new HashMap<K, V>();
                for (int i = 0; i < maxKeys && sourceIterator.hasNext(); ++i) {
                    var entry = sourceIterator.next();
                    batch.put(entry.getKey(), entry.getValue());
                }
                return batch;
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.DISTINCT), false);
    }

    public Stream<Set<K>> streamKeys(Set<K> source) {
        if (source == null) {
            return Stream.empty();
        }
        if (this.maxKeys < 1 || source.size() <= this.maxKeys) {
            return Stream.of(source);
        }
        var sourceIterator = source.iterator();
        Iterator<Set<K>> resultIterator = new Iterator<Set<K>>() {
            @Override
            public boolean hasNext() {
                return sourceIterator.hasNext();
            }

            @Override
            public Set<K> next() {
                var batch = new HashSet<K>();
                for (int i = 0; i < maxKeys && sourceIterator.hasNext(); ++i) {
                    batch.add(sourceIterator.next());
                }
                return batch;
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.DISTINCT), false);
    }
}
