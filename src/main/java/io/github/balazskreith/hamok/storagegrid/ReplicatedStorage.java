package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.CollectedStorageEvents;
import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.StorageEvents;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.UuidTools;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ReplicatedStorage<K, V> implements DistributedStorage<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorage.class);
    static final String PROTOCOL_NAME = "replicated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final Disposer disposer;
    private volatile boolean standalone;
    private final ReplicatedStorageConfig config;
    private final CollectedStorageEvents<K, V> collectedEvents;
    private final InputStreamer<K, V> inputStreamer;

    ReplicatedStorage(Storage<K, V> storage, StorageEndpoint<K, V> endpoint, ReplicatedStorageConfig config) {
        this.config = config;
        this.storage = storage;
        this.standalone = endpoint.getRemoteEndpointIds().size() < 1;
        this.inputStreamer = new InputStreamer<>(config.maxMessageKeys(), config.maxMessageValues());
        this.endpoint = endpoint
            .onGetKeysRequest(request -> {
                var keys = this.storage.keys();
                var response = request.createResponse(keys);
                this.endpoint.sendGetKeysResponse(response);
            })
            .onGetEntriesRequest(getEntriesRequest -> {
                var entries = this.storage.getAll(getEntriesRequest.keys());
                var response = getEntriesRequest.createResponse(entries);
                this.endpoint.sendGetEntriesResponse(response);
            })
            .onClearEntriesRequest(request -> {
                this.storage.clear();
                if (UuidTools.equals(this.endpoint.getLocalEndpointId(), request.sourceEndpointId())) {
                    var response = request.createResponse();
                    this.endpoint.sendClearEntriesResponse(response);
                }
            })
            .onInsertEntriesRequest(request -> {
                var entries = request.entries();
                Map<K, V> oldEntries = Collections.emptyMap();
                if (0 < entries.size()) {
                    oldEntries = this.storage.insertAll(entries);
                }
                logger.debug("Receiving request ({}) to insert {} entries from {} on endpoint {}", request.requestId(), entries.size(), request.sourceEndpointId(), this.endpoint.getLocalEndpointId());
                // only the same endpoint can resolve in replicated storage
                if (UuidTools.equals(this.endpoint.getLocalEndpointId(), request.sourceEndpointId())) {
                    var response = request.createResponse(oldEntries);
                    this.endpoint.sendInsertEntriesResponse(response);
                }

            })
            .onUpdateEntriesRequest(request -> {
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
            })
            .onDeleteEntriesRequest(request -> {
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
            })
            .onUpdateEntriesNotification(notification -> {
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
            })
            .onDeleteEntriesNotification(notification -> {
                // this notification should not occur in replicated storage
                logger.warn("Unexpected notification occurred in {}. request: {}", this.getClass().getSimpleName(), notification);
            })
            .onInsertEntriesNotification(notification -> {
                // this notification should not occur in replicated storage
                logger.warn("Unexpected notification occurred in {}. request: {}", this.getClass().getSimpleName(), notification);
            })
            .onRemoteEndpointDetached(remoteEndpointId -> {
                this.standalone = this.endpoint.getRemoteEndpointIds().size() < 1;
            })
            .onRemoteEndpointJoined(remoteEndpointId -> {

            })
            .onLeaderIdChanged(leaderIdHolder -> {
                if (leaderIdHolder.isEmpty()) {
                    return;
                }
                var wasAlone = this.standalone;
                this.standalone = false;
                if (!wasAlone) {
                    return;
                }
                // dumping items only we have!
                var keys = this.storage.keys();
                if (keys.isEmpty()) {
                    return;
                }
                var leaderId = leaderIdHolder.get();
                Set<K> remainingKeys;
                if (UuidTools.notEquals(leaderId, this.endpoint.getLocalEndpointId())) {
                    var remoteEntries = this.endpoint.requestGetEntries(keys, Set.of(leaderId));
                    if (0 < remoteEntries.size()) {
                        this.storage.setAll(remoteEntries);
                    }
                    remainingKeys = keys.stream().filter(key -> !remoteEntries.containsKey(key)).collect(Collectors.toSet());
                } else {
                    remainingKeys = keys;
                }

                if (0 < remainingKeys.size()) {
                    var remainingEntries = this.storage.getAll(remainingKeys);
                    this.inputStreamer.streamEntries(remainingEntries)
                            .forEach(requestedEntries -> this.endpoint.requestUpdateEntries(requestedEntries));
                    logger.info("Dumping {} entries into storage {} to be in sync with the cluster", remainingEntries.size(), this.getId());
                }
            });

        this.collectedEvents = this.storage.events()
                .collectOn(Schedulers.io(), config.maxCollectedActualStorageTimeInMs(), config.maxCollectedActualStorageEvents());

        this.disposer = Disposer.builder()
                .addDisposable(collectedEvents.expiredEntries().subscribe(modifiedStorageEntries -> {
                    if (!this.endpoint.isLeaderEndpoint()) {
                        // only the leader add entry about expired entries.
                        return;
                    }
                    var keys = modifiedStorageEntries.stream()
                            .map(entry -> entry.getKey())
                            .collect(Collectors.toSet());
                    this.endpoint.requestDeleteEntries(keys);
                }))
                .addDisposable(collectedEvents.evictedEntries().subscribe(modifiedStorageEntries -> {
                    // evicted items from local storage happens when clear is called.
                }))
                .build();
    }

    public CollectedStorageEvents<K, V> collectedEvents() {
        return this.collectedEvents;
    }

    @Override
    public String getId() {
        return this.config.storageId();
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
        return this.inputStreamer.streamEntries(entries)
                .map(requestedEntries -> this.endpoint.requestUpdateEntries(requestedEntries))
                .flatMap(respondedEntries -> respondedEntries.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
//        return this.endpoint.requestUpdateEntries(entries);
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
        return this.inputStreamer.streamKeys(keys)
                .map(requestedKeys -> this.endpoint.requestDeleteEntries(requestedKeys))
                .flatMap(respondedKeys -> respondedKeys.stream())
                .collect(Collectors.toSet());
//        Set<K> result = this.endpoint.requestDeleteEntries(keys);
//        return result;
    }


    /**
     *
     * @return
     */
    public Map<K, V> insertAll(Map<K, V> entries) {
        if (this.standalone) {
            return this.storage.insertAll(entries);
        }
        return this.inputStreamer.streamEntries(entries)
                .map(requestedEntries -> this.endpoint.requestInsertEntries(requestedEntries))
                .flatMap(respondedEntries -> respondedEntries.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
//        return this.endpoint.requestInsertEntries(entries);
    }

    @Override
    public boolean isEmpty() {
        return this.storage.isEmpty();
    }

    @Override
    public void clear() {
        if(this.standalone) {
            this.storage.clear();
            return;
        }
        this.endpoint.requestClearEntries();
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

    public boolean isJoined() {
        return this.standalone == false;
    }

    public ReplicatedStorageConfig getConfig() {
        return this.config;
    }

    StorageSyncResult executeSync() {
        var leaderId = this.endpoint.getLeaderId();
        if (leaderId == null) {
            return new StorageSyncResult(
                    true,
                    Collections.emptyList()
            );
        }

        try {
            var destinationIds = Set.of(leaderId);
            var keys = this.endpoint.requestGetKeys(destinationIds);
            var entries = this.inputStreamer.streamKeys(keys)
                    .flatMap(batchedKeys -> this.endpoint.requestGetEntries(batchedKeys, destinationIds).entrySet().stream())
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (v1, v2) -> {
                                return v2;
                            }
                    ));
            var storageSizeBefore = this.storage.size();
            this.storage.clear();
            this.storage.setAll(entries);
            var storageSizeAfter = this.storage.size();
            logger.info("Executed storage sync on {}. old storage size: {}, new storage size: {}, leader: {}",
                    this.storage.getId(),
                    storageSizeBefore,
                    storageSizeAfter,
                    leaderId
            );
            return new StorageSyncResult(
                    true,
                    Collections.emptyList()
            );
        } catch (Exception ex) {
            return new StorageSyncResult(
                    false,
                    List.of(ex.getMessage())
            );
        }
    }
}
