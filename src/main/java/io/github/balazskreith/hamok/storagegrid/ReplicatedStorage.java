package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.StorageEvents;
import io.github.balazskreith.hamok.common.UuidTools;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Federated storage assumes of multiple client modifies the same entry.
 * instead of lock every storage has its own version of the entry, and in case of a get request
 * a merge operation is applies to all entries requested
 * @param <K>
 * @param <V>
 */
public class ReplicatedStorage<K, V> implements DistributedStorage<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorage.class);
    static final String PROTOCOL_NAME = "replicated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final CompositeDisposable disposer;
    private volatile boolean standalone;
    private final ReplicatedStorageConfig config;

    ReplicatedStorage(Storage<K, V> storage, StorageEndpoint<K, V> endpoint, ReplicatedStorageConfig config) {
        this.config = config;
        this.storage = storage;
        this.standalone = endpoint.getRemoteEndpointIds().size() < 1;
        this.endpoint = endpoint
            .onGetEntriesRequest(getEntriesRequest -> {
                var entries = this.storage.getAll(getEntriesRequest.keys());
                var response = getEntriesRequest.createResponse(entries);
                this.endpoint.sendGetEntriesResponse(response);
            }).onDeleteEntriesRequest(request -> {
                var keys = request.keys();
                Set<K> deletedKeys = Collections.emptySet();
                if (0 < keys.size()) {
                    deletedKeys = this.storage.deleteAll(keys);
                }
                // only the same endpoint can resolve in replicated storage
                if (UuidTools.equals(this.endpoint.getLocalEndpointId(), request.sourceEndpointId())) {
                    var response = request.createResponse(deletedKeys);
                    this.endpoint.sendDeleteEntriesResponse(response);
                }
            }).onUpdateEntriesNotification(notification -> {
                // in storage sync this notification comes from the leader to
                // update all entries
                logger.info("{} updating storage {} by applying {} number of entries",
                        this.endpoint.getLocalEndpointId(),
                        this.getId(),
                        notification.entries().size()
                );
                if (0 < notification.entries().size()) {
                    this.storage.setAll(notification.entries());
                }
            }).onUpdateEntriesRequest(request -> {
                var entries = request.entries();
                Map<K, V> oldEntries = Collections.emptyMap();
                if (0 < entries.size()) {
                    oldEntries = this.storage.setAll(entries);
                }
                // only the same endpoint can resolve in replicated storage
                if (UuidTools.equals(this.endpoint.getLocalEndpointId(), request.sourceEndpointId())) {
                    var response = request.createResponse(oldEntries);
                    this.endpoint.sendUpdateEntriesResponse(response);
                }
            }).onDeleteEntriesNotification(notification -> {
                // this notification should not occur in replicated storage
                logger.warn("Unexpected notification occurred in {}. request: {}", this.getClass().getSimpleName(), notification);
            }).onInsertEntriesNotification(notification -> {
                // this notification should not occur in replicated storage
                logger.warn("Unexpected notification occurred in {}. request: {}", this.getClass().getSimpleName(), notification);
            }).onInsertEntriesRequest(request -> {
                var entries = request.entries();
                Map<K, V> oldEntries = Collections.emptyMap();
                if (0 < entries.size()) {
                    oldEntries = this.storage.insertAll(entries);
                }
                // only the same endpoint can resolve in replicated storage
                if (UuidTools.equals(this.endpoint.getLocalEndpointId(), request.sourceEndpointId())) {
                    var response = request.createResponse(oldEntries);
                    this.endpoint.sendInsertEntriesResponse(response);
                }

            }).onRemoteEndpointDetached(remoteEndpointId -> {
                this.standalone = this.endpoint.getRemoteEndpointIds().size() < 1;
            }).onRemoteEndpointJoined(remoteEndpointId -> {

            }).onLeaderIdChanged(leaderIdHolder -> {
                if (!this.standalone || leaderIdHolder.orElse(null) == null) {
                    // if the endpoint is already part of the cluster
                    // or the elected leader is null?! then we don't do anything
                }
                // we need to dump everything we have
                this.standalone = false;
                var keys = this.storage.keys();
                if (keys.isEmpty()) {
                    return;
                }
                var entries = this.storage.getAll(keys);
                var insertedEntries = this.endpoint.requestInsertEntries(entries);
                insertedEntries.keySet().stream()
                    .filter(key -> !keys.contains(key))
                    .forEach(key -> {
                        logger.warn("{} member tried to insert an already existing entry to the cluster after joined to the cluster. key: {}", this.storage.getId(), key);
                    });
            });

        this.disposer = new CompositeDisposable();
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
        return this.storage.get(key);
    }

    @Override
    public Map<K, V> getAll(Set<K> keys) {
        return this.storage.getAll(keys);
    }

    @Override
    public V set(K key, V value) {
        if (this.standalone) {
            return this.storage.set(key, value);
        }

        var updatedEntries = this.endpoint.requestUpdateEntries(Map.of(key, value));
        if (updatedEntries == null) {
            return null;
        }
        return updatedEntries.get(key);
    }

    @Override
    public Map<K, V> setAll(Map<K, V> entries) {
        if (this.standalone) {
            return this.storage.setAll(entries);
        }
        return this.endpoint.requestUpdateEntries(entries);
    }

    @Override
    public boolean delete(K key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (this.standalone) {
            return this.storage.delete(key);
        }
        var deletedKeys = this.endpoint.requestDeleteEntries(Set.of(key));
        if (deletedKeys == null) {
            return false;
        }
        return deletedKeys.contains(key);
    }

    @Override
    public Set<K> deleteAll(Set<K> keys) {
        Objects.requireNonNull(keys, "Keys cannot be null");
        if (keys.size() < 1) {
            return Collections.emptySet();
        } else if (this.standalone) {
            return this.storage.deleteAll(keys);
        }
        return this.endpoint.requestDeleteEntries(keys);
    }


    /**
     *
     * @return
     */
    public Map<K, V> insertAll(Map<K, V> entries) {
        if (this.standalone) {
            return this.storage.insertAll(entries);
        }
        return this.endpoint.requestInsertEntries(entries);
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
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
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
}
