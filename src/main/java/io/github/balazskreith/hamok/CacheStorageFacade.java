package io.github.balazskreith.hamok;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CacheStorageFacade<K, V> implements Storage<K, V> {
    private final Storage<K, V> storage;
    private final Storage<K, V> cache;

    public CacheStorageFacade(Storage<K, V> storage, Storage<K, V> cache) {
        this.storage = storage;
        this.cache = cache;
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
    public StorageEvents<K, V> events() {
        return this.storage.events();
    }

    @Override
    public Iterator<StorageEntry<K, V>> iterator() {
        return this.storage.iterator();
    }

    @Override
    public V get(K key) {
        var value = this.cache.get(key);
        if (value != null) {
            return value;
        }
        value = this.storage.get(key);
        if (value != null) {
            this.cache.set(key, value);
        }
        return value;
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        var cachedEntries = this.cache.getAll(keys);
        if (cachedEntries != null && cachedEntries.size() == keys.size()) {
            return cachedEntries;
        }
        var missingKeys = keys.stream()
                .filter(key -> !cachedEntries.containsKey(key))
                .collect(Collectors.toSet());
        var notCachedEntries = this.storage.getAll(missingKeys);
        if (notCachedEntries != null) {
            this.cache.setAll(notCachedEntries);
        }
        return Stream.concat(
                    cachedEntries.entrySet().stream(),
                    notCachedEntries.entrySet().stream()
                )
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
    }

    @Override
    public V set(K key, V value) {
        var result = this.storage.set(key, value);
        this.cache.evict(key);
        return result;
    }

    @Override
    public Map<K, V> setAll(Map<K, V> entries) {
        var result = this.storage.setAll(entries);
        this.cache.evictAll(entries.keySet());
        return result;
    }

    @Override
    public V insert(K key, V value) {
        var result = this.storage.insert(key, value);
        this.cache.evict(key);
        return result;
    }

    @Override
    public Map<K, V> insertAll(Map<K, V> entries) {
        var result = this.storage.insertAll(entries);
        this.cache.evictAll(entries.keySet());
        return result;
    }

    @Override
    public boolean delete(K key) {
        var result = this.storage.delete(key);
        this.cache.evict(key);
        return result;
    }

    @Override
    public Set<K> deleteAll(Set<K> keys) {
        var result = this.storage.deleteAll(keys);
        this.cache.evictAll(keys);
        return result;
    }

    @Override
    public void evict(K key) {
        this.storage.evict(key);
        this.cache.evict(key);
    }

    @Override
    public void evictAll(Set<K> keys) {
        this.storage.evictAll(keys);
        this.cache.evictAll(keys);
    }

    @Override
    public void restore(K key, V value) throws FailedOperationException {
        this.storage.restore(key, value);
        this.cache.evict(key);
    }

    @Override
    public void restoreAll(Map<K, V> entries) throws FailedOperationException {
        this.storage.restoreAll(entries);
        this.cache.evictAll(entries.keySet());
    }


    @Override
    public void close() throws Exception {
        this.cache.close();
        this.storage.close();
    }
}
