package io.github.balazskreith.hamok.storagegrid.backups;

import io.reactivex.rxjava3.core.Observable;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface BackupStorage<K, V> extends AutoCloseable {
    String PROTOCOL_NAME = "backup";

    static<U, R> BackupStorageBuilder<U, R> builder() {
        return new BackupStorageBuilder<>();
    }

    /**
     * Missing keys due to external events (remote endpoint detached)
     * @return
     */
    Observable<Set<K>> gaps();
    /**
     * Save entries on remote backups
     * @param entries
     */
    void save(Map<K, V> entries);

    /**
     * Delete entries saved on remote endpoint backup storages
     * @param keys
     */
    void delete(Set<K> keys);

    /**
     * Evict entries stored in local storage
     * @param keys
     */
    void evict(Set<K> keys);

    /**
     * Clear all entries from local backups
     */
    void clear();

    /**
     * extract all entries belongs to the specified endpoint and stored on local backup storage.
     * @param endpointId
     * @return
     */
    Map<K, V> extract(UUID endpointId);

    /**
     * Measurements and state snapshot representing the underlying backup storage
     * @return
     */
    BackupMetrics metrics();

}
