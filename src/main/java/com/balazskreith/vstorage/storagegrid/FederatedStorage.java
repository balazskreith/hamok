package com.balazskreith.vstorage.storagegrid;

import com.balazskreith.vstorage.Storage;
import com.balazskreith.vstorage.StorageEntry;
import com.balazskreith.vstorage.StorageEvents;
import com.balazskreith.vstorage.common.Disposer;
import com.balazskreith.vstorage.storagegrid.backups.BackupStorage;
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
public class FederatedStorage<K, V> implements Storage<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(FederatedStorage.class);

    static final String PROTOCOL_NAME = "federated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final BackupStorage<K, V> backupStorage;
    private final Disposer disposer;
    private final BinaryOperator<V> merge;

    FederatedStorage(Storage<K, V> storage, StorageEndpoint<K, V> endpoint, BackupStorage<K, V> backupStorage, BinaryOperator<V> merge) {
        this.merge = merge;
        this.backupStorage = backupStorage;
        this.storage = storage;
        this.endpoint = endpoint
            .onGetEntriesRequest(getEntriesRequest -> {
                var entries = this.storage.getAll(getEntriesRequest.keys());
                var response = getEntriesRequest.createResponse(entries);
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
                    this.storage.putAll(updatedEntries);
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
                    this.storage.putAll(updatedEntries);
                }
                var response = request.createResponse(oldEntries);
                this.endpoint.sendUpdateEntriesResponse(response);
            }).onDeleteEntriesNotification(notification -> {
                var keys = notification.keys();
                this.storage.deleteAll(keys);
            }).onRemoteEndpointDetached(remoteEndpointId -> {
                var savedEntries = this.backupStorage.extract(remoteEndpointId);
                this.storage.putAll(savedEntries);
            }).onLocalEndpointReset(payload -> {
                // we evict all keys from the local storage (it will not send
                // a notification to the remote endpoints, because the direct access of the local storage,
                // but it will send an event to the backup which evict all entries without saving it on another backup
                var keys = this.storage.keys();
                this.storage.evictAll(keys);
            });

        var collectedEvents = this.storage.events()
                .collectOn(Schedulers.io(), 100, 1000);
        this.disposer = Disposer.builder()
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
                    this.backupStorage.evict(keys);
                }))
                .build();
    }

    @Override
    public String getId() {
        return this.endpoint.getStorageId();
    }

    @Override
    public int size() {
        return this.storage.size();
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
    public V put(K key, V value) {
        return this.storage.put(key, value);
    }

    @Override
    public Map<K, V> putAll(Map<K, V> m) {
        return this.storage.putAll(m);
    }

    @Override
    public boolean delete(K key) {
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
        return this.storage.isEmpty();
    }

    @Override
    public void clear() {
        this.storage.clear();
    }

    @Override
    public Set<K> keys() {
        return this.storage.keys();
    }

    @Override
    public StorageEvents<K, V> events() {
        return this.storage.events();
    }

    @Override
    public Iterator<StorageEntry<K, V>> iterator() {
        return this.storage.iterator();
    }

    @Override
    public void close() throws Exception {
        this.storage.close();
        this.backupStorage.close();
        this.disposer.dispose();
    }
}
