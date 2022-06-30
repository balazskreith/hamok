package io.github.balazskreith.hamok;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

// scope is very important!
// delete is atomic locally, blablabla...

/**
 * General interface to represent a storage.
 *
 *
 * @param <K>
 * @param <V>
 */
public interface Storage<K, V> extends AutoCloseable {

    String getId();
    /**
     * The number of entries the storage has
     *
     * @return The number of entries the Storage has
     */
    int size();

    /**
     *
     * @return true if the storage has not value
     */
    boolean isEmpty();

    /**
     * Delete all entries
     */
    void clear();

    /**
     * Return all keys the storage has
     * @return
     */
    Set<K> keys();

    /**
     * Events of the storage
     * @return
     */
    StorageEvents<K, V> events();

    /**
     * returns an iterator for the storage.
     *
     * Whether the iterator directly iterates through the storage or only a set of copy is implementation dependent
     *
     * @return an iterator for the storage
     */
    Iterator<StorageEntry<K, V>> iterator();

    /**
     * Read a value from the storage belongs to a given key.
     *
     * @param key the key to which a value belongs to
     * @return the corresponding value or null if it does not found a value.
     */
    default V get(K key) {
        var result = this.getAll(Set.of(key));
        return result.get(key);
    }

    /**
     * Gets all entries for the given keys.
     *
     * The method must return with non-null value.
     *
     * @param keys the given keys
     * @return found entries.
     */
    Map<K, V> getAll(Set<K> keys);

    /**
     * Updates the storage for the given key,value pair.
     * If the key has not been existed, this method creates the entry. If the entry is existed
     * this method updates the value for the key and returns the previous value
     *
     * @param key The key of the entry
     * @param value The value of the entry
     * @return the previous value if the key existed before, or null if it is a new value
     */
    default V set(K key, V value) {
        var result = this.setAll(Map.of(key, value));
        if (result == null) return null;
        return result.get(key);
    }


    /**
     * Update / Create entries of the storage.
     *
     * The method must not return null value
     *
     * @param map the entries to update or create
     * @return the map of entries overriden by the operation
     */
    Map<K, V> setAll(Map<K, V> map);

    default V insert(K key, V value) {
        var existingEntries = this.insertAll(Map.of(key, value));
        if (existingEntries == null) return null;
        return existingEntries.get(key);
    }

    /**
     * Insert operation ensures the atomicity of adding a key value pair to the storage.
     * Not all storage types supporting this operation
     * @param entries
     * @return
     */
    Map<K, V> insertAll(Map<K, V> entries);

    /**
     * Delete entry belong to the given key
     *
     * @param key
     * @return true if the key existed, false if it was not
     */
    default boolean delete(K key) {
        var result = this.deleteAll(Set.of(key));
        return result.contains(key);
    }

    /**
     * Delete all entries
     *
     * The method must not return null value
     *
     * @param keys
     * @return the set of keys deleted from the storage
     */
    Set<K> deleteAll(Set<K> keys);

    /**
     * Evict an entry from a storage
     *
     * @param key
     */
    default void evict(K key) {
        this.evictAll(Set.of(key));
    }

    /**
     * Evict all entries belong to the given keys
     *
     * The difference between evict and delete are the followings:
     *  - delete requires a reply from the storage about the deleted keys, evict does not
     *  - evict generates evicted events, thus the interpretation of the event may be different
     *
      * @param keys
     */
    void evictAll(Set<K> keys);
}
