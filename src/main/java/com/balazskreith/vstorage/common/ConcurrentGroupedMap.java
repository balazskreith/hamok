package com.balazskreith.vstorage.common;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrentGroupedMap<G, K, V> {

    private final RwLock rwLock = new RwLock();
    private final Map<G, Set<K>> groups = new HashMap<>();
    private final Map<K, V> entries = new HashMap<>();

    public int size() {
        return this.rwLock.supplyInReadLock(this.entries::size);
    }

    public boolean isEmpty() {
        return this.rwLock.supplyInReadLock(this.entries::isEmpty);
    }

    public boolean containsKey(Object key) {
        return this.rwLock.supplyInReadLock(() -> this.entries.containsKey(key));
    }

    public boolean containsValue(Object value) {
        return this.rwLock.supplyInReadLock(() -> this.entries.containsValue(value));
    }

    public V get(Object key) {
        return this.rwLock.supplyInReadLock(() -> this.entries.get(key));
    }

    public List<V> getGroup(G group) {
        var result = this.rwLock.supplyInReadLock(() -> this.groups.getOrDefault(group, Collections.emptySet())
                .stream()
                .map(key -> this.entries.get(key))
                .filter(Objects::nonNull))
                .collect(Collectors.toList())
                ;
        return result;
    }

    public Stream<V> stream(G group) {
        var list = this.getGroup(group);
        return list.stream();
    }

    public V put(G group, K key, V value) {
        return this.rwLock.supplyInWriteLock(() -> {
            var groupKeys = this.groups.get(group);
            if (groupKeys == null) {
                groupKeys = new HashSet<>();
                this.groups.put(group, groupKeys);
            }
            groupKeys.add(key);
            return this.entries.put(key, value);
        });
    }

    public V remove(Object key) {
        return this.rwLock.supplyInWriteLock(() -> {
            this.groups.values().forEach(keys -> keys.remove(key));
            return this.entries.remove(key);
        });
    }

    public void clear() {
        this.rwLock.runInWriteLock(() -> {
            this.entries.clear();
            this.groups.clear();
        });
    }

    public Set<K> keySet() {
        var set  = this.rwLock.supplyInReadLock(() -> Set.copyOf(this.entries.keySet()));
        return set;
    }

    public Collection<V> values() {
        return this.rwLock.supplyInReadLock(() -> List.copyOf(this.entries.values()));
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return this.rwLock.supplyInReadLock(() -> Set.copyOf(this.entries.entrySet()));
    }

    public void forEach(BiConsumer<? super K, ? super V> action) {
        var entries = this.entrySet();
        entries.forEach(entry -> action.accept(entry.getKey(), entry.getValue()));
    }

}
