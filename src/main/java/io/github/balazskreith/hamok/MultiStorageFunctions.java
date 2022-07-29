package io.github.balazskreith.hamok;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

class MultiStorageFunctions<K, V, U extends Collection<V>> {
    private static final Logger logger = LoggerFactory.getLogger(MultiStorageFunctions.class);

    private final Storage<K, U> storage;
    private final BiFunction<U, List<V>, U> addition;
    private final BiFunction<U, List<V>, U> subtraction;
    private final BiFunction<U, V, Boolean> contains;

    MultiStorageFunctions(Storage<K, U> storage,
                          BiFunction<U, List<V>, U> addition,
                          BiFunction<U, List<V>, U> subtraction,
                          BiFunction<U, V, Boolean> contains
    ) {
        this.storage = storage;
        this.addition = addition;
        this.subtraction = subtraction;
        this.contains = contains;
    }


    public U add(K key, V... values) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (values == null || values.length < 1) {
            return this.storage.get(key);
        }
        U collection = this.storage.get(key);
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
        var collection = this.storage.get(key);
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
            return this.storage.get(key);
        }
        var collection = this.subtraction.apply(this.storage.get(key), List.of(values));
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
}
