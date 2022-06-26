package io.github.balazskreith.vstorage.storagegrid;

import io.github.balazskreith.vstorage.Storage;
import io.github.balazskreith.vstorage.StorageEntry;
import io.github.balazskreith.vstorage.StorageEvents;
import io.github.balazskreith.vstorage.common.Disposer;
import io.github.balazskreith.vstorage.common.MapUtils;
import io.github.balazskreith.vstorage.common.SetUtils;
import io.github.balazskreith.vstorage.storagegrid.backups.BackupStorage;
import io.github.balazskreith.vstorage.storagegrid.messages.EvictEntriesNotification;
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
public class SeparatedStorage<K, V> implements Storage<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorage.class);
    static final String PROTOCOL_NAME = "separated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final BackupStorage<K, V> backupStorage;
    private final Disposer disposer;

    SeparatedStorage(
            Storage<K, V> storage,
            StorageEndpoint<K, V> endpoint,
            BackupStorage<K, V> backupStorage
    ) {
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
                // we evict all keys from the local storage (it will not send an
                // a notification to the remote endpoints, because the direct access of the local storage
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
        var remoteEntries = this.endpoint.requestGetEntries(keys);
        if (remoteEntries != null) {
            result.putAll(remoteEntries);
        }
        return result;
    }

    @Override
    public V put(K key, V value) {
        if (this.storage.get(key) != null) {
            return this.storage.put(key, value);
        }
        // update requests from remote cannot create new items! (at least for this storage type
        var updatedRemoteEntries = this.endpoint.requestUpdateEntries(Map.of(key, value));
        if (updatedRemoteEntries != null && updatedRemoteEntries.containsKey(key)) {
            return updatedRemoteEntries.get(key);
        }
        return this.storage.put(key, value);
    }

    @Override
    public Map<K, V> putAll(Map<K, V> m) {
        var updatedLocalEntries = this.storage.getAll(m.keySet());
        var missingKeys = new HashSet<>(m.keySet());
        if (0 < updatedLocalEntries.size()) {
            var updatedEntries = updatedLocalEntries.entrySet().stream()
                    .collect(Collectors.toMap(
                            entry -> entry.getKey(),
                            entry -> m.get(entry.getKey())
                    ));
            this.storage.putAll(updatedEntries);
            updatedLocalEntries.keySet().stream().forEach(missingKeys::remove);
        }
        if (missingKeys.size() < 1) {
            return updatedLocalEntries;
        }
        var remainingEntries = missingKeys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> m.get(key)
        ));
        var updatedRemoteEntries = this.endpoint.requestUpdateEntries(remainingEntries);
        if (updatedRemoteEntries != null && 0 < updatedRemoteEntries.size()) {
            updatedRemoteEntries.keySet().stream().forEach(missingKeys::remove);
        }
        var result = MapUtils.putAll(updatedLocalEntries, updatedRemoteEntries);
        if (missingKeys.size() < 1) {
            return result;
        }
        var newEntries = missingKeys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> m.get(key)
        ));
        this.storage.putAll(newEntries);
        return result;
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
        var remoteDeletedKeys = this.endpoint.requestDeleteEntries(remainingKeys);
        return SetUtils.addAll(localDeletedKeys, remoteDeletedKeys);
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
        var notification = new EvictEntriesNotification<K>(keys, null, null);
        this.endpoint.sendEvictedEntriesNotification(notification);
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
