package io.github.balazskreith.hamok;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StorageBatchedIterator<K, V> implements Iterator<StorageEntry<K, V>>, AutoCloseable {

    private final int storageSize;
    private final int batchSize;
    private final List<K> keys;
    private Storage<K, V> storage;

    private boolean closed = false;
    private int index = 0;
    private Map<Integer, StorageEntry<K, V>> batch;


    public StorageBatchedIterator(Storage<K, V> storage, int batchSize) {
        this.storage = storage;
        this.keys = storage.keys().stream().collect(Collectors.toList());
        this.storageSize = storage.size();
        this.batchSize = batchSize;
    }

    @Override
    public void close() throws Exception {
        this.closed = true;
        this.storage = null;
        this.keys.clear();
    }

    @Override
    public boolean hasNext() {
        return this.closed == false && this.index < this.storageSize;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Iterator cannot change the storage");
    }

    @Override
    public StorageEntry<K, V> next() {
        if (this.closed) {
            return null;
        }
        if (this.batch == null || this.batch.containsKey(this.index) == false) {
            this.batch = new HashMap<>();
            var toIndex = Math.min(this.index + this.batchSize, this.storageSize);
            var batchKeys = this.keys.subList(this.index, toIndex).stream().collect(Collectors.toSet());
            var fetchedEntries = this.storage.getAll(batchKeys);
            var it = fetchedEntries.entrySet().iterator();
            var savedIndex = this.index;
            for (; it.hasNext(); ++savedIndex) {
                var entry = it.next();
                var storageEntry = StorageEntry.createFromMapEntry(entry);
                this.batch.put(savedIndex, storageEntry);
            }
        }
        var result = this.batch.get(this.index);
        ++this.index;
        return result;
    }
}
