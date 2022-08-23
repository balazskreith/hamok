package io.github.balazskreith.hamok;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// scope is very important!
// delete is atomic locally, blablabla...

/**
 * General interface to represent a storage.
 *
 *
 * @param <K>
 * @param <V>
 */
public interface MultiStorageSet<K, V> extends MultiStorage<K, V, Set<V>> {

    static<E, R> MultiStorageSet<E, R> fromStorage(Storage<E, Set<R>> storage) {
        var multiStorageMethods = new MultiStorageFunctions<E, R, Set<R>>(
                storage,
                (set, toAdd) -> Stream.concat(set.stream(), toAdd.stream()).collect(Collectors.toSet()),
                (set, toRemove) -> set.stream().filter(value -> toRemove.stream().anyMatch(candidate -> !set.contains(candidate))).collect(Collectors.toSet()),
                (set, value) -> set.contains(value)
        );
        return new MultiStorageSet<E, R>() {
            @Override
            public Set<R> add(E key, R... values) {
                return multiStorageMethods.add(key, values);
            }

            @Override
            public Map<E, Set<R>> addAll(Map<E, Set<R>> entries) {
                return multiStorageMethods.addAll(entries);
            }

            @Override
            public boolean contains(E key, R... values) {
                return multiStorageMethods.contains(key, values);
            }

            @Override
            public boolean containsAll(Map<E, Set<R>> entries) {
                return multiStorageMethods.containsAll(entries);
            }

            @Override
            public boolean containsAny(Map<E, Set<R>> entries) {
                return multiStorageMethods.containsAny(entries);
            }

            @Override
            public Set<R> remove(E key, R... values) {
                return multiStorageMethods.remove(key, values);
            }

            @Override
            public Map<E, Set<R>> removeAll(Map<E, Set<R>> entries) {
                return multiStorageMethods.removeAll(entries);
            }

            @Override
            public String getId() {
                return storage.getId();
            }

            @Override
            public int size() {
                return storage.size();
            }

            @Override
            public boolean isEmpty() {
                return storage.isEmpty();
            }

            @Override
            public void clear() {
                storage.clear();
            }

            @Override
            public Set<E> keys() {
                return storage.keys();
            }

            @Override
            public StorageEvents<E, Set<R>> events() {
                return storage.events();
            }

            @Override
            public Iterator<StorageEntry<E, Set<R>>> iterator() {
                return storage.iterator();
            }

            @Override
            public Map<E, Set<R>> getAll(Set<E> keys) {
                return storage.getAll(keys);
            }

            @Override
            public Map<E, Set<R>> setAll(Map<E, Set<R>> map) {
                return storage.setAll(map);
            }

            @Override
            public Map<E, Set<R>> insertAll(Map<E, Set<R>> entries) {
                return storage.insertAll(entries);
            }

            @Override
            public Set<E> deleteAll(Set<E> keys) {
                return storage.deleteAll(keys);
            }

            @Override
            public void evictAll(Set<E> keys) {
                storage.evictAll(keys);
            }

            @Override
            public void restoreAll(Map<E, Set<R>> entries) {
                storage.restoreAll(entries);
            }

            @Override
            public void close() throws Exception {
                storage.close();
            }
        };
    }

}
