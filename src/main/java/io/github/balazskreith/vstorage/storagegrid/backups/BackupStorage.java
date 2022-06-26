package io.github.balazskreith.vstorage.storagegrid.backups;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface BackupStorage<K, V> extends AutoCloseable {
    String PROTOCOL_NAME = "backup";

    static<U, R> BackupStorageBuilder<U, R> builder() {
        return new BackupStorageBuilder<>();
    }

    /**
     * Save entries on remote backups
     * @param entries
     */
    void save(Map<K, V> entries);

    /**
     * Delete entries from local and remote backups
     * @param keys
     */
    void delete(Set<K> keys);

    /**
     * Evict entries from local backups.
     * @param keys
     */
    void evict(Set<K> keys);

    /**
     * Clear all entries from local backups and send notification to remote backups
     */
    void clear();

    /**
     * extract all entries belongs to the specified endpoint and stored on local backup storage.
     * @param endpointId
     * @return
     */
    Map<K, V> extract(UUID endpointId);
}
