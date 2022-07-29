package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.StorageEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

class DistributedMultiStorageFromDistributedStorage<K, V, U extends Collection<V>> implements DistributedMultiStorage<K, V, U> {
    private static final Logger logger = LoggerFactory.getLogger(DistributedMultiStorageFromDistributedStorage.class);

    private final DistributedStorage<K, U> storage;
    private final BiFunction<U, List<V>, U> addition;
    private final BiFunction<U, List<V>, U> subtraction;
    private final BiFunction<U, V, Boolean> contains;

    DistributedMultiStorageFromDistributedStorage(DistributedStorage<K, U> storage,
                                                  BiFunction<U, List<V>, U> addition,
                                                  BiFunction<U, List<V>, U> subtraction,
                                                  BiFunction<U, V, Boolean> contains
    ) {
        this.storage = storage;
        this.addition = addition;
        this.subtraction = subtraction;
        this.contains = contains;
    }

    @Override
    public String getId() {
        return this.storage.getId();
    }

    @Override
    public int size() {
        return this.storage.size();
    }

    @Override
    public U get(K key) {
        return this.storage.get(key);
    }

    @Override
    public Map<K, U> getAll(Set<K> keys) {
        return this.storage.getAll(keys);
    }

    @Override
    public U set(K key, U value) {
        return this.storage.set(key, value);
    }

    public U add(K key, V... values) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (values == null || values.length < 1) {
            return this.get(key);
        }
        U collection = this.get(key);
        collection = this.addition.apply(collection, List.of(values));
        this.storage.set(key, collection);
        return collection;
    }

    public Map<K, U> addAll(Map<K, U> entries) {
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyMap();
        }
        var oldValues = this.storage.getAll(entries.keySet());
        if (oldValues == null || oldValues.isEmpty()) {
            this.storage.setAll(entries);
            return entries;
        }
        var result = new HashMap<K, U>();
        for (var entry : entries.entrySet()) {
            var key = entry.getKey();
            var collection = oldValues.get(key);
            if (collection == null) {
                result.put(key, entry.getValue());
            } else {
                collection = this.addition.apply(collection, entry.getValue().stream().toList());
                result.put(key, collection);
            }
        }
        this.storage.setAll(result);
        return result;
    }

    public boolean contains(K key, V... values) {
        Objects.requireNonNull(key, "Key cannot be null");
        var collection = this.get(key);
        if (collection == null) {
            return false;
        }
        if (values == null || values.length < 1) {
            return true;
        }
        return Stream.of(values).allMatch(value -> this.contains.apply(collection, value));
    }

    public boolean containsAll(Map<K, U> entries) {
        if (entries == null || entries.isEmpty())  {
            return true;
        }
        var collections = this.storage.getAll(entries.keySet());
        for (var entry : entries.entrySet()) {
            var collection = collections.get(entry.getKey());
            if (collection == null) {
                return false;
            }
            var values = entry.getValue();
            if (values == null || values.size() < 1) {
                continue;
            }
            var containsAll = values.stream().allMatch(value -> this.contains.apply(collection, value));
            if (!containsAll) {
                return false;
            }
        }
        return true;
    }

    public boolean containsAny(Map<K, U> entries) {
        if (entries == null || entries.isEmpty())  {
            return true;
        }
        var collections = this.storage.getAll(entries.keySet());
        for (var entry : entries.entrySet()) {
            var collection = collections.get(entry.getKey());
            if (collection == null) {
                continue;
            }
            var values = entry.getValue();
            if (values == null || values.size() < 1) {
                continue;
            }
            var containsAny = values.stream().anyMatch(value -> this.contains.apply(collection, value));
            if (containsAny) {
                return true;
            }
        }
        return false;
    }

    public U remove(K key, V... values) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (values == null || values.length < 1) {
            return this.get(key);
        }
        var collection = this.subtraction.apply(this.get(key), List.of(values));
        if (collection == null || collection.size() < 1) {
            this.storage.delete(key);
        } else {
            this.storage.set(key, collection);
        }
        return collection;
    }

    public Map<K, U> removeAll(Map<K, U> entries) {
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyMap();
        }
        var oldValues = this.storage.getAll(entries.keySet());
        var result = new HashMap<K, U>();
        for (var entry : entries.entrySet()) {
            var key = entry.getKey();
            var toRemove = entry.getValue();
            var collection = oldValues.get(key);
            if (toRemove == null || collection == null) {
                continue;
            }
            collection = this.subtraction.apply(collection, toRemove.stream().toList());
            result.put(key, collection);
        }
        this.storage.setAll(result);
        return result;
    }

    @Override
    public Map<K, U> setAll(Map<K, U> entries) {
        return this.storage.setAll(entries);
    }

    @Override
    public boolean delete(K key) {
        return this.storage.delete(key);
    }

    @Override
    public Set<K> deleteAll(Set<K> keys) {
        return this.storage.deleteAll(keys);
    }


    public Map<K, U> insertAll(Map<K, U> entries) {
        return this.storage.insertAll(entries);
    }

    @Override
    public boolean isEmpty() {
        return this.storage.isEmpty();
    }

    @Override
    public void clear() {
        this.storage.clear();
    }

    @Override
    public Set<K> keys() {
        return this.storage.keys();
    }

    @Override
    public StorageEvents<K, U> events() {
        return this.storage.events();
    }

    @Override
    public Iterator<StorageEntry<K, U>> iterator() {
        return this.storage.iterator();
    }

    @Override
    public void close() throws Exception {
        this.storage.close();
    }

    @Override
    public boolean localIsEmpty() {
        return this.storage.localIsEmpty();
    }

    @Override
    public int localSize() {
        return this.storage.localSize();
    }

    @Override
    public Set<K> localKeys() {
        return this.storage.localKeys();
    }

    @Override
    public Iterator<StorageEntry<K, U>> localIterator() {
        return this.storage.localIterator();
    }

    @Override
    public void localClear() {
        this.storage.localClear();
    }
}
