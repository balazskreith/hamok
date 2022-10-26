package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.*;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.SetUtils;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This type of storage assumes every key is modified by one and only one endpoint.
 * The entries are saved in the storage first appears and later on only that storage (until the endpoint is up)
 * modifies that entry
 * @param <K>
 * @param <V>
 */
public class SeparatedStorage<K, V> implements DistributedStorage<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorage.class);
    static final String PROTOCOL_NAME = "separated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final SeparatedStorageConfig config;
    private final Disposer disposer;
    private final CollectedStorageEvents<K, V> collectedEvents;
    private final InputStreamer<K, V> inputStreamer;

    SeparatedStorage(
            Storage<K, V> storage,
            StorageEndpoint<K, V> endpoint,
            SeparatedStorageConfig config
    ) {
        this.config = config;
        this.storage = storage;
        this.endpoint = endpoint
            .onGetSizeRequest(getSizeRequest -> {
                var size = this.storage.size();
                var response = getSizeRequest.createResponse(size);
                this.endpoint.sendGetSizeResponse(response);
            })
            .onGetEntriesRequest(getEntriesRequest -> {
                var entries = this.storage.getAll(getEntriesRequest.keys());
                var response = getEntriesRequest.createResponse(entries);
                this.endpoint.sendGetEntriesResponse(response);
            })
            .onGetKeysRequest(request -> {
                var response = request.createResponse(
                        this.storage.keys()
                );
                this.endpoint.sendGetKeysResponse(response);
            })
            .onDeleteEntriesRequest(request -> {
                var deletedKeys = this.storage.deleteAll(request.keys());
                var response = request.createResponse(deletedKeys);
                this.endpoint.sendDeleteEntriesResponse(response);
            })
            .onUpdateEntriesNotification(notification -> {
                var entries = notification.entries();

                // only update entries what we have!
                var existingKeys = this.storage.getAll(entries.keySet()).keySet();
                var updatedEntries = entries.entrySet().stream()
                        .filter(entry -> existingKeys.contains(entry.getKey()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (v1, v2) -> {
                                    logger.error("Duplicated item tried to be merged at {} storage {} operation: {}, {}", this.storage.getId(), "onUpdateEntriesNotification", v1, v2);
                                    return v1;
                                }
                        ));
                if (0 < updatedEntries.size()) {
                    this.storage.setAll(updatedEntries);
                }
            }).onUpdateEntriesRequest(request -> {
                var entries = request.entries();

                // only update entries what we have!
                var oldEntries = this.storage.getAll(entries.keySet());
                var updatedEntries = entries.entrySet().stream()
                        .filter(entry -> oldEntries.containsKey(entry.getKey()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (v1, v2) -> {
                                    logger.error("Duplicated item tried to be merged at {} storage {} operation: {}, {}", this.storage.getId(), "onUpdateEntriesRequest", v1, v2);
                                    return v1;
                                }
                        ));
                if (0 < updatedEntries.size()) {
                    this.storage.setAll(updatedEntries);
                }
                var response = request.createResponse(oldEntries);
                this.endpoint.sendUpdateEntriesResponse(response);
            }).onDeleteEntriesNotification(notification -> {
                var keys = notification.keys();
                this.storage.deleteAll(keys);
            });

        this.collectedEvents = this.storage.events()
                .collectOn(Schedulers.io(), this.config.maxCollectedActualStorageTimeInMs(), this.config.maxCollectedActualStorageEvents());
        this.disposer = Disposer.builder().build();
        this.inputStreamer = new InputStreamer<>(config.maxMessageKeys(), config.maxMessageValues());
    }

    public CollectedStorageEvents<K, V> collectedEvents() {
        return this.collectedEvents;
    }

    @Override
    public String getId() {
        return this.endpoint.getStorageId();
    }

    @Override
    public int size() {
        return this.storage.size() + this.endpoint.requestGetSize();
    }

    @Override
    public V get(K key) {
        var result = this.storage.get(key);
        if (result != null) {
            return result;
        }
        var remoteEntries = this.endpoint.requestGetEntries(Set.of(key));
        if (remoteEntries != null) {
            result = remoteEntries.get(key);
        }
        return result;
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        Map<K, V> result = new HashMap<>();
        var localEntries = this.storage.getAll(keys);
        if (localEntries != null) {
            result.putAll(localEntries);
        }
        if (keys.size() <= result.size()) {
            return result;
        }
        return this.inputStreamer.streamKeys(keys)
                .map(requestedKeys -> this.endpoint.requestGetEntries(requestedKeys))
                .flatMap(respondedEntries -> respondedEntries.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> {
                            logger.error("Duplicated item tried to be merged at {} storage {} operation: {}, {}", this.storage.getId(), "getAll", v1, v2);
                            return v1;
                        }
                ));
    }

    @Override
    public V set(K key, V value) {
        if (this.storage.get(key) != null) {
            return this.storage.set(key, value);
        }
        // update requests from remote cannot create new items! (at least for this storage type
        var updatedRemoteEntries = this.endpoint.requestUpdateEntries(Map.of(key, value));
        if (updatedRemoteEntries != null && updatedRemoteEntries.containsKey(key)) {
            return updatedRemoteEntries.get(key);
        }
        return this.storage.set(key, value);
    }

    @Override
    public Map<K, V> setAll(Map<K, V> m) {
        var updatedLocalEntries = this.storage.getAll(m.keySet());
        var missingKeys = new HashSet<>(m.keySet());
        if (0 < updatedLocalEntries.size()) {
            var updatedEntries = updatedLocalEntries.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> m.get(entry.getKey())
                    ));
            this.storage.setAll(updatedEntries);
            updatedLocalEntries.keySet().stream().forEach(missingKeys::remove);
        }
        if (missingKeys.size() < 1) {
            return updatedLocalEntries;
        }
        var remainingEntries = missingKeys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> m.get(key)
        ));

        var updatedRemoteEntries = this.inputStreamer.streamEntries(remainingEntries)
                .map(requestedEntries -> this.endpoint.requestUpdateEntries(requestedEntries))
                .flatMap(respondedEntries -> respondedEntries.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> {
                            logger.error("Duplicated item tried to be merged at storage {} operation: setAll. Values: {}, {}", this.storage.getId(), v1, v2);
                            return v1;
                        }
                ));
        if (updatedRemoteEntries != null && 0 < updatedRemoteEntries.size()) {
            updatedRemoteEntries.keySet().stream().forEach(missingKeys::remove);
        }

        var result = MapUtils.combineAll(updatedLocalEntries, updatedRemoteEntries);
        if (missingKeys.size() < 1) {
            return result;
        }
        var newEntries = missingKeys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> m.get(key)
        ));
        this.storage.setAll(newEntries);
        return result;
    }

    @Override
    public Map<K, V> insertAll(Map<K, V> entries) {
        var existingLocalEntries = this.storage.getAll(entries.keySet());
        var missingKeys = new HashSet<>(entries.keySet());
        if (0 < existingLocalEntries.size()) {
            existingLocalEntries.keySet().forEach(missingKeys::remove);
        }
        if (missingKeys.size() < 1) {
            return existingLocalEntries;
        }
        var existingRemoteEntries = this.inputStreamer.streamKeys(missingKeys)
                .map(requestedKeys -> this.endpoint.requestGetEntries(requestedKeys))
                .flatMap(respondedEntries -> respondedEntries.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (v1, v2) -> {
                            logger.error("Duplicated item tried to be merged at {} storage {} operation: {}, {}", this.storage.getId(), "insertAll", v1, v2);
                            return v1;
                        }
                ));
        if (0 < existingRemoteEntries.size()) {
            existingRemoteEntries.keySet().stream().forEach(missingKeys::remove);
        }
        var result = MapUtils.combineAll(existingLocalEntries, existingRemoteEntries);
        if (missingKeys.size() < 1) {
            return result;
        }
        var newEntries = missingKeys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> entries.get(key)
        ));
        return MapUtils.combineAll(result, this.storage.insertAll(newEntries));
    }

    @Override
    public boolean delete(K key) {
        if (this.storage.delete(key)) {
            return true;
        }
        var deletedKeys = this.endpoint.requestDeleteEntries(Set.of(key));
        return deletedKeys != null && deletedKeys.contains(key);
    }

    @Override
    public Set<K> deleteAll(Set<K> keys) {
        if (keys.size() < 1) {
            return Collections.emptySet();
        }
        var localDeletedKeys = this.storage.deleteAll(keys);
        if (localDeletedKeys.size() == keys.size()) {
            return localDeletedKeys;
        }
        Set<K> remainingKeys;
        if (localDeletedKeys.size() < 1) {
            remainingKeys = keys;
        } else {
            remainingKeys = keys.stream()
                    .filter(key -> !localDeletedKeys.contains(key))
                    .collect(Collectors.toSet());
        }
        var remoteDeletedKeys = this.inputStreamer.streamKeys(remainingKeys)
                .map(requestedKeys -> this.endpoint.requestDeleteEntries(requestedKeys))
                .flatMap(respondedEntries -> respondedEntries.stream())
                .collect(Collectors.toSet());
        return SetUtils.combineAll(localDeletedKeys, remoteDeletedKeys);
    }

    @Override
    public boolean isEmpty() {
        return this.storage.isEmpty() && this.endpoint.requestGetSize() < 1;
    }

    @Override
    public void clear() {
        this.storage.clear();
    }

    @Override
    public Set<K> keys() {
        var remoteKeys = this.endpoint.requestGetKeys();
        return SetUtils.combineAll(this.storage.keys(), remoteKeys);
    }

    @Override
    public StorageEvents<K, V> events() {
        return this.storage.events();
    }

    @Override
    public Iterator<StorageEntry<K, V>> iterator() {
        return new StorageBatchedIterator<>(this, this.config.iteratorBatchSize());
    }

    @Override
    public void close() throws Exception {
        this.storage.close();
        this.disposer.dispose();
    }

    @Override
    public void evict(K key) {
        throw new RuntimeException("evict operation is not allowed");
    }

    @Override
    public void evictAll(Set<K> keys) {
        throw new RuntimeException("evict operation is not allowed");
    }

    @Override
    public void restoreAll(Map<K, V> entries) {
        throw new RuntimeException("restore operation is not allowed for replicated storage");
    }

    @Override
    public boolean localIsEmpty() {
        return this.storage.isEmpty();
    }

    @Override
    public int localSize() {
        return this.storage.size();
    }

    @Override
    public Set<K> localKeys() {
        return this.storage.keys();
    }

    @Override
    public Iterator<StorageEntry<K, V>> localIterator() {
        return this.storage.iterator();
    }

    @Override
    public void localClear() {
        this.storage.clear();
    }

    public SeparatedStorageConfig getConfig() {
        return this.config;
    }

    public void checkCollidingEntries() throws FailedOperationException {
        var localKeys = this.storage.keys();
        if (localKeys == null || localKeys.size() < 1) {
            return;
        }
        var remoteEndpointIds = this.endpoint.getRemoteEndpointIds();
        if (remoteEndpointIds == null || remoteEndpointIds.size() < 1) {
            return;
        }
        var collidingKeysToRemoveRemotely = new HashSet<K>();
        for (var remoteEndpointId : remoteEndpointIds) {
            Map<K, V> remoteEntries;
            remoteEntries = this.endpoint.requestGetEntries(localKeys, Set.of(remoteEndpointId));
            if (remoteEntries == null || remoteEntries.size() < 1) {
                continue;
            }
            var collidingKeys = localKeys.stream()
                    .filter(localKey -> remoteEntries.containsKey(localKey))
                    .collect(Collectors.toSet());

            if (collidingKeys.size() < 1) {
                continue;
            }
            // who should keep the collidingEntries?
            if (this.endpoint.getLocalEndpointId().getMostSignificantBits() < remoteEndpointId.getMostSignificantBits()) {
                this.storage.deleteAll(collidingKeys);
            } else {
                collidingKeysToRemoveRemotely.addAll(collidingKeys);
            }
        }
        if (0 < collidingKeysToRemoveRemotely.size()) {
            this.endpoint.requestDeleteEntries(collidingKeysToRemoveRemotely);
        }
    }

    StorageSyncResult executeSync() {
        var remoteEndpointIds = this.endpoint.getRemoteEndpointIds();
        boolean success = true;
        var removedEntries = 0;
        var errors = new LinkedList<String>();
        var localKeys = this.storage.keys();
        if (localKeys.size() < 1) {
            logger.info("Storage Sync has been performed on storage: {}. Removed Entries: 0, new local storage size: {}", this.getId(), removedEntries, this.storage.size());
            return new StorageSyncResult(
                    true,
                    Collections.emptyList()
            );
        }
        for (var remoteEndpointId : remoteEndpointIds) {
            Map<K, V> remoteEntries;
            try {
                remoteEntries = this.endpoint.requestGetEntries(localKeys, Set.of(remoteEndpointId));
            } catch (Exception ex) {
                success = false;
                errors.add(ex.getMessage());
                continue;
            }
            var collidingEntries = remoteEntries.entrySet()
                    .stream()
                    .filter(remoteEntry -> localKeys.contains(remoteEntry.getKey()))
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
            if (0 < collidingEntries.size()) {
                this.storage.deleteAll(collidingEntries.keySet());
                removedEntries += collidingEntries.size();
            }
        }
        logger.info("Storage Sync has been performed on storage: {}. Removed Entries: {}, new local storage size: {}", this.getId(), removedEntries, this.storage.size());
        return new StorageSyncResult(
                success,
                errors
        );
    }
}
