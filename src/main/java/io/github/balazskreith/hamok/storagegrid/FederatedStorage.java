
package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.StorageBatchedIterator;
import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.StorageEvents;
import io.github.balazskreith.hamok.common.BatchCollector;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorage;
import io.github.balazskreith.hamok.storagegrid.messages.ClearEntriesNotification;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
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
                this.endpoint.sendGetEntriesResponse(response);
            }).onDeleteEntriesRequest(request -> {
                var deletedKeys = this.storage.deleteAll(request.keys());
                var response = request.createResponse(deletedKeys);
                this.endpoint.sendDeleteEntriesResponse(response);
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
            }).onClearEntriesNotification(notification -> {
                var keys = this.storage.keys();
                this.storage.clear();
                this.backupStorage.delete(keys);
            }).onRemoteEndpointJoined(remoteEndpointId -> {

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
                logger.debug("{} detected remote endpoint {} detached. extracted entries from backup: {}, localEntries: {}, updatedEntries: {}",
                        this.endpoint.getLocalEndpointId(),
                        remoteEndpointId,
                        savedEntries.size(),
                        localEntries.size(),
                        updatedEntries.size()
                );
                this.storage.setAll(updatedEntries);
            }).onLocalEndpointReset(payload -> {
                var evictedEntries = this.storage.size();
                this.storage.clear();
                var backupMetrics = this.backupStorage.metrics();
                this.backupStorage.clear();
                logger.info("{} Reset Storage {}. Evicted storage entries: {}, Deleted backup entries: {}",
                        this.endpoint.getLocalEndpointId(), this.storage.getId(), evictedEntries, backupMetrics.storedEntries());
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
                    // evicted items from local storage happens when clear is called.
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
        var remoteEntries = new HashMap<K, V>();
        keys.stream().collect(this.makeBatchCollectorForKeys(batchedKeys -> {
            var remoteEntriesBatch = this.endpoint.requestGetEntries(batchedKeys);
            if (remoteEntriesBatch != null) {
                remoteEntries.putAll(remoteEntriesBatch);
            }
        }));
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
        var oldValue = this.storage.get(key);
        if (oldValue == null) {
            return this.storage.set(key, value);
        }
        var newValue = this.merge.apply(oldValue, value);
        return this.storage.set(key, newValue);
    }

    @Override
    public Map<K, V> setAll(Map<K, V> m) {
        if (m == null || m.isEmpty()) {
            return Collections.emptyMap();
        }
        var oldValues = this.storage.getAll(m.keySet());
        if (oldValues == null || oldValues.isEmpty()) {
            return this.storage.setAll(m);
        }
        var newValues = new HashMap<K, V>();
        for (var entry : m.entrySet()) {
            var key = entry.getKey();
            var oldValue = oldValues.get(key);
            if (oldValue == null) {
                newValues.put(key, entry.getValue());
            } else {
                var newValue = this.merge.apply(oldValue, entry.getValue());
                newValues.put(key, newValue);
            }
        }
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
    public boolean isEmpty() {
        if (!this.storage.isEmpty()) {
            return false;
        }
        return this.endpoint.requestGetSize() < 1;
    }

    @Override
    public void clear() {
        this.storage.clear();
        var notification = new ClearEntriesNotification(this.endpoint.getLocalEndpointId());
        this.endpoint.sendClearEntriesNotification(notification);
    }

    @Override
    public Set<K> keys() {
        Set<K> remoteKeys = this.endpoint.requestGetKeys();
        return SetUtils.combineAll(remoteKeys, this.storage.keys());
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
    public Iterator<StorageEntry<K, V>> localIterator() {
        return this.storage.iterator();
    }

    @Override
    public void localClear() {
        this.storage.clear();
    }

    public FederatedStorageConfig getConfig() {
        return this.config;
    }

    private BatchCollector<K, Set<K>> makeBatchCollectorForKeys(Consumer<Set<K>> consumer) {
        return BatchCollector.builder()
                .withEmptyCollectionSupplier(Collections::emptySet)
                .withMutableCollectionSupplier(HashSet::new)
                .withBatchSize(Math.min(this.config.maxMessageKeys(), this.config.maxMessageValues()))
                .withConsumer(consumer)
                .build();
    }

    private BatchCollector<Map.Entry<K, V>, List<Map.Entry<K, V>>> makeBatchCollectorForEntrySet(Consumer<List<Map.Entry<K, V>>> consumer) {
        var batchSize = Math.min(this.config.maxMessageKeys(), this.config.maxMessageValues());
        return BatchCollector.builder()
                .withEmptyCollectionSupplier(Collections::emptyList)
                .withMutableCollectionSupplier(ArrayList::new)
                .withBatchSize(batchSize)
                .withConsumer(consumer)
                .build();
    }
}
