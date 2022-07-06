package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.StorageBatchedIterator;
import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.StorageEvents;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorage;
import io.github.balazskreith.hamok.storagegrid.messages.ClearEntriesNotification;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Federated storage assumes of multiple client modifies the same entry.
 * instead of lock every storage has its own version of the entry, and in case of a get request
 * a merge operation is applied to all entries responded a get request
 * @param <K>
 * @param <V>
 */
public class FederatedStorage<K, V> implements DistributedStorage<K, V> {
    private static final int ITERATOR_BATCH_SIZE = 10000;
    private static final Logger logger = LoggerFactory.getLogger(FederatedStorage.class);

    static final String PROTOCOL_NAME = "federated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private final FederatedStorageConfig config;

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final BackupStorage<K, V> backupStorage;
    private final Disposer disposer;
    private final BinaryOperator<V> merge;

    FederatedStorage(Storage<K, V> storage, StorageEndpoint<K, V> endpoint, BackupStorage<K, V> backupStorage, BinaryOperator<V> merge, FederatedStorageConfig config) {
        this.merge = merge;
        this.backupStorage = backupStorage;
        this.storage = storage;
        this.config = config;
        this.endpoint = endpoint
            .onGetEntriesRequest(getEntriesRequest -> {
                var entries = this.storage.getAll(getEntriesRequest.keys());
                var response = getEntriesRequest.createResponse(entries);
                logger.info("{} get response: {}", this.endpoint.getLocalEndpointId(), response);
                this.endpoint.sendGetEntriesResponse(response);
            }).onDeleteEntriesRequest(request -> {
                var deletedKeys = this.storage.deleteAll(request.keys());
                var response = request.createResponse(deletedKeys);
                this.endpoint.sendDeleteEntriesResponse(response);
            }).onEvictedEntriesNotification(request -> {
                this.storage.evictAll(request.keys());
            }).onUpdateEntriesNotification(notification -> {
                var entries = notification.entries();

                // only update entries what we have!
                var existingKeys = this.storage.getAll(entries.keySet()).keySet();
                var updatedEntries = entries.entrySet().stream()
                        .filter(entry -> existingKeys.contains(entry.getKey()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
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
                                Map.Entry::getValue
                        ));
                if (0 < updatedEntries.size()) {
                    this.storage.setAll(updatedEntries);
                }
                var response = request.createResponse(oldEntries);
                this.endpoint.sendUpdateEntriesResponse(response);
            }).onDeleteEntriesNotification(notification -> {
                var keys = notification.keys();
                this.storage.deleteAll(keys);
            }).onGetSizeRequest(request -> {
                var response = request.createResponse(
                        this.storage.size()
                );
                this.endpoint.sendGetSizeResponse(response);
            }).onGetKeysRequest(request -> {
                var response = request.createResponse(
                        this.storage.keys()
                );
                this.endpoint.sendGetKeysResponse(response);
            }).onRemoteEndpointJoined(remoteEndpointId -> {
//                if (this.endpoint.getRemoteEndpointIds().size() == 1) {
//                    // this is the first time any endpoint joined
//                    var localKeys = this.storage.keys();
//                    var entries = this.storage.getAll(localKeys);
//                    this.backupStorage.save(entries);
//                }
            }).onRemoteEndpointDetached(remoteEndpointId -> {
                var savedEntries = this.backupStorage.extract(remoteEndpointId);
                if (savedEntries.isEmpty()) {
                    return;
                }
                var localEntries = this.storage.getAll(savedEntries.keySet());
                var depot = MapUtils.<K, V>makeMergedMapDepot(this.merge);
                depot.accept(localEntries);
                depot.accept(savedEntries);
                var updatedEntries = depot.get();
                logger.info("{} detected remote endpoint {} detached. extracted entries from backup: {}, localEntries: {}, updatedEntries: {}",
                        this.endpoint.getLocalEndpointId(),
                        remoteEndpointId,
                        JsonUtils.objectToString(savedEntries),
                        JsonUtils.objectToString(localEntries),
                        JsonUtils.objectToString(updatedEntries)
                );
                this.storage.setAll(updatedEntries);
            }).onLocalEndpointReset(payload -> {
                // at this point we know that this endpoint became inactive, and it is reinstated now.
                // we need to request all keys from all remote endpoints, and evict them from here, because remote endpoints
                // fetched our previous keys from their backup. if this endpoint has some key intersected with remote and changed locally after,
                // the changes will be lost.
                // all new keys created in this endpoint after it was detached will remain in the possession of the storage and
                // accessible for other storages.
                var remoteKeys = this.endpoint.requestGetKeys();
                this.storage.evictAll(remoteKeys);
            });

        var collectedEvents = this.storage.events()
                .collectOn(Schedulers.io(), config.maxCollectedActualStorageTimeInMs(), config.maxCollectedActualStorageEvents());

        this.disposer = Disposer.builder()
                .addDisposable(this.backupStorage.gaps().subscribe(missingKeys -> {
                    var entries = this.storage.getAll(missingKeys);
                    if (0 < entries.size()) {
                        this.backupStorage.save(entries);
                    }
                }))
                .addDisposable(collectedEvents.createdEntries().subscribe(modifiedStorageEntries -> {
                    var entries = modifiedStorageEntries.stream().collect(Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> entry.getNewValue()
                    ));
                    this.backupStorage.save(entries);
                }))
                .addDisposable(collectedEvents.updatedEntries().subscribe(modifiedStorageEntries -> {
                    var entries = modifiedStorageEntries.stream().collect(Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> entry.getNewValue()
                    ));
                    this.backupStorage.save(entries);
                }))
                .addDisposable(collectedEvents.deletedEntries().subscribe(modifiedStorageEntries -> {
                    var keys = modifiedStorageEntries.stream()
                            .map(entry -> entry.getKey())
                            .collect(Collectors.toSet());
                    this.backupStorage.delete(keys);
                }))
                .addDisposable(collectedEvents.expiredEntries().subscribe(modifiedStorageEntries -> {
                    var keys = modifiedStorageEntries.stream()
                            .map(entry -> entry.getKey())
                            .collect(Collectors.toSet());
                    this.backupStorage.delete(keys);
                }))
                .addDisposable(collectedEvents.evictedEntries().subscribe(modifiedStorageEntries -> {
                    var keys = modifiedStorageEntries.stream()
                            .map(entry -> entry.getKey())
                            .collect(Collectors.toSet());
                    this.backupStorage.delete(keys);
                }))
                .build();
    }


    @Override
    public String getId() {
        return this.endpoint.getStorageId();
    }

    @Override
    public int size() {
        return this.keys().size();
    }

    @Override
    public V get(K key) {
        var localValue = this.storage.get(key);
        var remoteValue = this.endpoint.requestGetEntries(Set.of(key)).get(key);
        if (localValue != null && remoteValue != null) {
            return this.merge.apply(localValue, remoteValue);
        } else if (localValue != null) {
            return localValue;
        } else if (remoteValue != null) {
            return remoteValue;
        }
        return null;
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        Map<K, V> result = new HashMap<>();
        var localEntries = this.storage.getAll(keys);
        var remoteEntries = this.endpoint.requestGetEntries(keys);
        if (localEntries.size() < 1 && remoteEntries.size() < 1) {
            return Collections.emptyMap();
        } else if (localEntries.size() < 1) {
            return remoteEntries;
        } else if (remoteEntries.size() < 1) {
            return localEntries;
        }
        return Stream.concat(localEntries.entrySet().stream(), remoteEntries.entrySet().stream()).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                this.merge
        ));
    }

    @Override
    public V set(K key, V value) {
        return this.storage.set(key, value);
    }

    @Override
    public Map<K, V> setAll(Map<K, V> m) {
        return this.storage.setAll(m);
    }

    @Override
    public Map<K, V> insertAll(Map<K, V> entries) {
        return this.storage.insertAll(entries);
    }

    @Override
    public boolean delete(K key) {
//        logger.info("Delete key: {}, value: {}", key, this.storage.get(key));
        return this.storage.delete(key);
    }

    @Override
    public Set<K> deleteAll(Set<K> keys) {
        if (keys.size() < 1) {
            return Collections.emptySet();
        }
        return this.storage.deleteAll(keys);
    }

    @Override
    public void evict(K key) {
        this.evictAll(Set.of(key));
    }

    @Override
    public void evictAll(Set<K> keys) {
        if (keys.size() < 1) {
            return;
        }
        this.storage.evictAll(keys);
    }

    @Override
    public boolean isEmpty() {
        if (!this.storage.isEmpty()) {
            return false;
        }
        return this.endpoint.requestGetSize() < 1;
    }

    @Override
    public void clear() {
        var notification = new ClearEntriesNotification();
        this.endpoint.sendClearEntriesNotification(notification);
        this.storage.clear();
    }

    @Override
    public Set<K> keys() {
        Set<K> remoteKeys = this.endpoint.requestGetKeys();
        return SetUtils.addAll(remoteKeys, this.storage.keys());
    }

    @Override
    public StorageEvents<K, V> events() {
        return this.storage.events();
    }

    @Override
    public Iterator<StorageEntry<K, V>> iterator() {
        return new StorageBatchedIterator<>(this, ITERATOR_BATCH_SIZE);
    }

    @Override
    public void close() throws Exception {
        this.storage.close();
        this.backupStorage.close();
        this.disposer.dispose();
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
    public void localClear() {
        this.storage.clear();
    }

    @Override
    public Iterator<StorageEntry<K, V>> localIterator() {
        return this.storage.iterator();
    }
}
