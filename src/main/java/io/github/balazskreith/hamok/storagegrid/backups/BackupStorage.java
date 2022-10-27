package io.github.balazskreith.hamok.storagegrid.backups;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface BackupStorage<K, V> {

    /**
     * Save entries
     * @param entries
     */
    void save(Map<K, V> entries);

    /**
     * load entries from destination endpoint id
     * @param destinationEndpointId
     * @return
     */
    Map<K, Optional<V>> loadFrom(UUID destinationEndpointId);

    /**
     * Delete entries saved on remote endpoint backup storages
     * @param keys
     */
    void delete(Set<K> keys);

    /**
     * Load entries saved on this
     * @param endpointId
     * @return
     */
    Map<K, V> extract(UUID endpointId);

    /**
     * Evict entries stored in local storage belongs to a source endpointId
     * @param keys
     */
    void evict(Set<K> keys, UUID sourceEndpointId);

    /**
     * Clear all entries from local backups
     */
    void clear();

}
