package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.StorageEvents;
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
public class ReplicatedStorage<K, V> implements Storage<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorage.class);
    static final String PROTOCOL_NAME = "replicated-storage";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, V> endpoint;
    private final Storage<K, V> storage;
    private final CompositeDisposable disposer;
    private volatile boolean standalone;

    ReplicatedStorage(Storage<K, V> storage, StorageEndpoint<K, V> endpoint) {
        this.storage = storage;
        this.standalone = endpoint.getRemoteEndpointIds().size() < 1;
        this.endpoint = endpoint
            .onGetEntriesRequest(getEntriesRequest -> {
                var entries = this.storage.getAll(getEntriesRequest.keys());
                var response = getEntriesRequest.createResponse(entries);
                this.endpoint.sendGetEntriesResponse(response);
            }).onDeleteEntriesRequest(request -> {
                // only leader can do it

                var deletedKeys = this.storage.deleteAll(request.keys());
                var response = request.createResponse(deletedKeys);
                this.endpoint.sendDeleteEntriesResponse(response);
            }).onEvictedEntriesNotification(request -> {
                // only for local eviction
                this.storage.evictAll(request.keys());
            }).onUpdateEntriesNotification(notification -> {
                // only follower should do this

                var entries = notification.entries();
                this.storage.setAll(entries);
            }).onUpdateEntriesRequest(request -> {
                // only leader can do this

                var entries = request.entries();
                var oldEntries = this.storage.setAll(entries);
                var response = request.createResponse(oldEntries);
                this.endpoint.sendUpdateEntriesResponse(response);
            }).onDeleteEntriesNotification(notification -> {
                // only follower should do this

                var keys = notification.keys();
                this.storage.deleteAll(keys);
            }).onRemoteEndpointDetached(remoteEndpointId -> {
                this.standalone = 0 < this.endpoint.getRemoteEndpointIds().size();
            }).onRemoteEndpointJoined(remoteEndpointId -> {
                // we need to dump the whole map when number of endpoints got more than itself.
            }).onLeaderIdChanged(leaderIdHolder -> {
                if (leaderIdHolder.orElse(null) != null && this.standalone) {
                    // we need to dump everything we have
                    this.standalone = false;
                    var keys = this.storage.keys();
                    var entries = this.storage.getAll(keys);
                    this.endpoint.submitRequestUpdateEntries(entries);
                }
            }).onInsertEntriesNotification(notification -> {
                this.storage.setAll(notification.entries());
            }).onInsertEntriesRequest(request -> {
                var response = request.createResponse(this.storage.insertAll(request.entries()));
                this.endpoint.sendInsertEntriesResponse(response);
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
        // we need to send it for commit and wait for the response
        var oldEntries = this.endpoint.submitRequestUpdateEntries(Map.of(key, value));
        if (oldEntries == null) return null;
        return oldEntries.get(key);
    }

    @Override
    public Map<K, V> setAll(Map<K, V> m) {
        if (this.standalone) {
            return this.storage.setAll(m);
        }
        return this.endpoint.submitRequestUpdateEntries(m);
    }

    @Override
    public boolean delete(K key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (this.standalone) {
            return this.storage.delete(key);
        }
        var deletedKeys = this.endpoint.submitRequestDeleteEntries(Set.of(key));
        if (deletedKeys == null) return false;
        return deletedKeys.contains(key);
    }

    @Override
    public Set<K> deleteAll(Set<K> keys) {
        if (keys.size() < 1) {
            return Collections.emptySet();
        } else if (this.standalone) {
            return this.storage.deleteAll(keys);
        }
        return this.endpoint.submitRequestDeleteEntries(keys);
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

    /**
     *
     * @return
     */
    public Map<K, V> insertAll(Map<K, V> entries) {
        return this.endpoint.submitRequestInsertEntries(entries);

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
}
