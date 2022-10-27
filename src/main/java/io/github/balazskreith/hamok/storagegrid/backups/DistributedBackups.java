package io.github.balazskreith.hamok.storagegrid.backups;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public interface DistributedBackups extends AutoCloseable {
    String PROTOCOL_NAME = "distributed-backup";

    final Logger logger = LoggerFactory.getLogger(DistributedBackups.class);

    String getId();

    /**
     * Save key and value pair on a destination endpoint.
     *
     * @param key the key of the entry
     * @param value the value for the entry
     */
    void save(String key, String value);

    /**
     * load entries from destination endpoint
     * @param destinationEndpointId
     * @return
     */
    Map<String, Optional<String>> loadFrom(UUID destinationEndpointId);

    /**
     * delete an entry on a remote endpoint
     * @param key the key of the entry
     */
    void delete(String key);

    /**
     * Store key, and value on this endpoint originated from a remote endpoint
     * @param key
     * @param value
     * @param sourceEndpointId
     */
    void store(String key, String value, UUID sourceEndpointId);

    /**
     * extract all entry stored at this endpoint and belongs to a specified endpoint
     * @param endpointId
     * @return list of stored entries, or an empty list
     */
    Map<String, String> extract(UUID endpointId);

    /**
     * Evict all keys saved from a specified endpoint.
     * @param keys
     */
    void evict(Set<String> keys, UUID endpointId);


    default<K, V> BackupStorage<K, V> createAdapter(
            String storageId,
            Function<K, byte[]> keyEncoder,
            Function<byte[], K> keyDecoder,
            Function<V, byte[]> valueEncoder,
            Function<byte[], V> valueDecoder
    ) {
        final String DELIMITER = "-::-";
        final String STORAGE_ID = storageId.replace(DELIMITER, "");
        var base64Encoder = Base64.getEncoder();
        var base64Decoder = Base64.getDecoder();
        var savedBackupKeys = Collections.synchronizedSet(new HashSet<String>());
        return new BackupStorage<K, V>() {
            @Override
            public void save(Map<K, V> entries) {
                for (var entry : entries.entrySet()) {
                    var key = entry.getKey();
                    var value = entry.getValue();
                    try {
                        var encodedKey = keyEncoder.apply(key);
                        var backupKey = String.join(DELIMITER, STORAGE_ID, base64Encoder.encodeToString(encodedKey));
                        var encodedValue = valueEncoder.apply(value);
                        var backupValue = base64Encoder.encodeToString(encodedValue);
                        DistributedBackups.this.save(backupKey, backupValue);
                        savedBackupKeys.add(backupKey);
                    } catch (Exception ex) {
                        logger.warn("Error occurred while trying to save backup for key {} in storage {}.", key, STORAGE_ID, ex);
                        continue;
                    }
                }
            }

            @Override
            public Map<K, Optional<V>> loadFrom(UUID destinationEndpointId) {
                var loadedEntries = DistributedBackups.this.loadFrom(destinationEndpointId);
                var result = new HashMap<K, Optional<V>>();
                for (var entry : loadedEntries.entrySet()) {
                    var backupKey = entry.getKey();
                    var backupValue = entry.getValue();
                    try {
                        var split = backupKey.split(DELIMITER);
                        if (split == null || split.length != 2) {
                            logger.warn("Expected to split key {} based on delimiter \"{}\", but it returned null or less/more than 2 items", backupKey, DELIMITER);
                            continue;
                        }
                        var base64Key = base64Decoder.decode(split[1]);
                        var key = keyDecoder.apply(base64Key);
                        if (backupValue == null || backupValue.isEmpty()) {
                            result.put(key, Optional.empty());
                            continue;
                        }
                        var base64Value = base64Decoder.decode(backupValue.get());
                        var value = valueDecoder.apply(base64Value);
                        result.put(key, Optional.of(value));
                    } catch (Exception ex) {
                        logger.warn("Error occurred while trying to load backup for key: {}, storage: {}", backupKey, STORAGE_ID, ex);
                        continue;
                    }
                }
                return result;
            }

            @Override
            public void delete(Set<K> keys) {
                for (var key : keys) {
                    try {
                        var encodedKey = keyEncoder.apply(key);
                        var backupKey = String.join(DELIMITER, STORAGE_ID, base64Encoder.encodeToString(encodedKey));
                        DistributedBackups.this.delete(backupKey);
                        savedBackupKeys.remove(backupKey);
                    } catch (Exception ex) {
                        logger.warn("Error occurred while trying to delete backup for key {} in storage {}.", key, STORAGE_ID, ex);
                        continue;
                    }
                }
            }

            @Override
            public Map<K, V> extract(UUID endpointId) {
                var entries = DistributedBackups.this.extract(endpointId);
                if (entries == null || entries.size() < 1) {
                    return Collections.emptyMap();
                }
                var result = new HashMap<K, V>();
                for (var entry : entries.entrySet()) {
                    var backupKey = entry.getKey();
                    var backupValue = entry.getValue();
                    try {
                        var split = backupKey.split(DELIMITER);
                        if (split == null || split.length != 2) {
                            logger.warn("Expected to split key {} based on delimiter \"{}\", but it returned null or less/more than 2 items", backupKey, DELIMITER);
                            continue;
                        }
                        var base64Key = base64Decoder.decode(split[1]);
                        var key = keyDecoder.apply(base64Key);
                        var base64Value = base64Decoder.decode(backupValue);
                        var value = valueDecoder.apply(base64Value);
                        result.put(key, value);
                    } catch (Exception ex) {
                        logger.warn("Error occurred while trying to load backup for key: {}, storage: {}", backupKey, STORAGE_ID, ex);
                        continue;
                    }
                }
                return result;
            }

            @Override
            public void evict(Set<K> keys, UUID sourceEndpointId) {
                if (keys == null || keys.size() < 1) {
                    return;
                }
                var backupKeys = new HashSet<String>();
                for (var key : keys) {
                    try {
                        var encodedKey = keyEncoder.apply(key);
                        var backupKey = String.join(DELIMITER, STORAGE_ID, base64Encoder.encodeToString(encodedKey));
                        backupKeys.add(backupKey);
                    } catch (Exception ex) {
                        logger.warn("Error occurred while encoding key {} to a backupkey for storage {}", key, STORAGE_ID, ex);
                        continue;
                    }
                }
                DistributedBackups.this.evict(backupKeys, sourceEndpointId);
            }

            @Override
            public void clear() {
                DistributedBackups.this.evict(savedBackupKeys, null);
                savedBackupKeys.clear();
            }
        };
    }

}
