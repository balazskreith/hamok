
package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.StorageEvent;
import io.github.balazskreith.hamok.StorageEventDispatcher;
import io.github.balazskreith.hamok.StorageEvents;
import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.storagegrid.messages.ClearEntriesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.RemoveEntriesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.UpdateEntriesNotification;
import io.reactivex.rxjava3.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Federated storage assumes of multiple client modifies the same entry.
 * instead of lock every storage has its own version of the entry, and in case of a get request
 * a merge operation is applied to all entries responded a get request
 * @param <K>
 * @param <V>
 */
@Deprecated
public class PropagatedCollections<K, V, T extends Collection<V>> implements Disposable {
    private static final int ITERATOR_BATCH_SIZE = 10000;
    private static final Logger logger = LoggerFactory.getLogger(PropagatedCollections.class);

    static final String PROTOCOL_NAME = "distributed-collection";

    public static<U, R> SeparatedStorageBuilder<U, R> builder() {
        return new SeparatedStorageBuilder<>();
    }

    private StorageEndpoint<K, T> endpoint;
    private final Map<K, T> collections;
    private final Supplier<T> newCollection;
    private final RwLock rwLock = new RwLock();
    private final PropagatedCollectionsConfig config;
    private final StorageEventDispatcher<K, T> events = new StorageEventDispatcher<K, T>();

    PropagatedCollections(StorageEndpoint<K, T> endpoint, Supplier<T> newCollection, PropagatedCollectionsConfig config) {
        this.newCollection = newCollection;
        this.config = config;
        this.collections = new HashMap<>();
        this.endpoint = endpoint
            .onRemoveEntriesNotification(notification -> {

                var entries = notification.entries();
                this.deleteAll(entries);

            }).onUpdateEntriesNotification(notification -> {

                var entries = notification.entries();
                this.updateAll(entries);

            }).onClearEntriesNotification(notification -> {

                this.clear();

            }).onGetKeysRequest(request -> {

                var keys = this.collections.keySet();
                var response = request.createResponse(keys);
                this.endpoint.sendGetKeysResponse(response);

            }).onRemoteEndpointJoined(remoteEndpointId -> {
                var entries = new HashMap<K, T>();
                this.rwLock.runInReadLock(() -> {
                    for (var entry : this.collections.entrySet()) {
                        var collection = newCollection.get();
                        collection.addAll(entry.getValue());
                        entries.put(entry.getKey(), collection);
                    }
                });
                var notification = new UpdateEntriesNotification<K, T>(
                        entries,
                        this.endpoint.getLocalEndpointId(),
                        remoteEndpointId
                );
                this.endpoint.sendUpdateEntriesNotification(notification);
            }).onLocalEndpointReset(payload -> {
                this.rwLock.runInWriteLock(() -> {
                    this.collections.clear();
                });
            });

    }

    public StorageEvents<K, T> events() {
        return this.events;
    }

    public int size() {
        return this.rwLock.supplyInReadLock(this.collections::size);
    }

    public T get(K key) {
        return this.rwLock.supplyInReadLock(() -> this.collections.get(key));
    }

    public Map<K, T> getAll(Set<K> keys) {
        return this.rwLock.supplyInReadLock(() -> {
            var result = new HashMap<K, T>();
            for (var key : keys) {
                var values = this.collections.get(key);
                if (values == null) {
                    continue;
                }
                result.put(key, values);
            }
            return Collections.unmodifiableMap(result);
        });
    }

    public void add(K key, V... values) {
        var collection = newCollection.get();
        Stream.of(values).forEach(collection::add);
        var entries =  Map.of(key, collection);
        this.updateAll(entries);
        var notification = new UpdateEntriesNotification<K, T>(
                entries,
                this.endpoint.getLocalEndpointId(),
                null
        );
        this.endpoint.sendUpdateEntriesNotification(notification);
    }

    public void addAll(Map<K, T> entries) {
        this.updateAll(entries);
        var notification = new UpdateEntriesNotification<K, T>(
                entries,
                this.endpoint.getLocalEndpointId(),
                null
        );
        this.endpoint.sendUpdateEntriesNotification(notification);
    }

    public boolean remove(K key, V... values) {
        var collection = newCollection.get();
        Stream.of(values).forEach(collection::add);
        var removedCollections = this.deleteAll(Map.of(key, collection));
        if (removedCollections == null) {
            return false;
        }
        var removedCollection = removedCollections.get(key);
        if (removedCollection == null) {
            return false;
        }
        var result = removedCollection.containsAll(collection);
        if (result) {

        }
        return result;
    }

    public Map<K, T> removeAll(Map<K, T> entries) {
        var deletedEntries = this.deleteAll(entries);
        var notification = new RemoveEntriesNotification<K, T>(
                deletedEntries,
                this.endpoint.getLocalEndpointId()
        );
        this.endpoint.sendRemoveEntriesNotification(notification);
        return deletedEntries;
    }

    public boolean contains(K key, V... values) {
        return this.rwLock.supplyInReadLock(() -> {
            var collection = this.collections.get(key);
            if (collection == null) {
                return false;
            }
            return Stream.of(values).allMatch(collection::contains);
        });
    }

    public boolean containsAll(Map<K, T> entries) {
        return this.rwLock.supplyInReadLock(() -> {
            for (var entry : entries.entrySet()) {
                var key = entry.getKey();
                var subjects = entry.getValue();
                var collection = this.collections.get(key);
                if (collection == null) {
                    return false;
                }
                if (!collection.containsAll(subjects)) {
                    return false;
                }
            }
            return true;
        });
    }

    private void updateAll(Map<K, T> entries) {
        this.rwLock.runInWriteLock(() -> {
            var insertedEntries = new HashMap<K, T>();
            for (var entry : entries.entrySet()) {
                var collection = this.collections.get(entry.getKey());
                if (collection == null) {
                    collection = newCollection.get();
                    insertedEntries.put(entry.getKey(), collection);
                }
                collection.addAll(entry.getValue());
            }
            if (0 < insertedEntries.size()) {
                this.collections.putAll(insertedEntries);
            }
        });
    }

    private Map<K, T> deleteAll(Map<K, T> entries) {
        var result = new HashMap<K, T>();
        this.rwLock.runInWriteLock(() -> {
            var toRemove = new HashSet<K>();
            for (var entry : entries.entrySet()) {
                var key = entry.getKey();
                var removedValues = entry.getValue();
                var existingValues = this.collections.get(key);
                if (existingValues == null) {
                    continue;
                }
                if (removedValues == null) {
                    logger.warn("Attempted to pass null collection as collection of values to be removed");
                    continue;
                }
                for (var removedValue : removedValues) {
                    if (!existingValues.remove(removedValue)) {
                        continue;
                    }
                    var resultCollection = result.get(key);
                    if (resultCollection == null) {
                        resultCollection = newCollection.get();
                        result.put(key, resultCollection);
                    }
                    resultCollection.add(removedValue);
                }
                if (existingValues.size() < 1) {
                    toRemove.add(key);
                }
            }
            if (0 < toRemove.size()) {
                toRemove.forEach(this.collections::remove);
            }
        });
        return result;
    }

    public boolean isEmpty() {
        return this.collections.isEmpty();
    }

    public void clear() {
        this.collections.clear();
        var notification = new ClearEntriesNotification(this.endpoint.getLocalEndpointId());
        this.endpoint.sendClearEntriesNotification(notification);
    }

    public Set<K> keys() {
        return this.collections.keySet();
    }

    public Iterator<Map.Entry<K, T>> iterator() {
        return this.rwLock.supplyInReadLock(() -> Set.copyOf(this.collections.entrySet()).iterator());
    }

    private boolean executeSync() {
        return true;
//        var remoteKeys = this.endpoint.requestGetKeys();
//        this.rwLock.runInWriteLock(() -> {
//            remoteKeys.forEach(this.collections::remove);
//        });
//        logger.info("Storage {} is synced. Evicted storage entries: {}",
//                this.endpoint.getStorageId(),
//                remoteKeys.size()
//        );
//        return true;
    }

    static<U, R, E extends Collection<R>> GridActor createGridMember(PropagatedCollections<U, R, E> subject) {
        return GridActor.builder()
                .setIdentifier(subject.endpoint.getStorageId())
                .setMessageAcceptor(subject.endpoint::receive)
                .setCloseAction(() -> subject.endpoint.dispose())
                .setSyncExecutor(subject::executeSync)
                .build();
    }

    @Override
    public void dispose() {
        if (this.events.isDisposed()) {
            return;
        }
        this.clear();
        this.events.accept(StorageEvent.makeClosingStorageEvent(this.endpoint.getStorageId()));
        this.events.dispose();
    }

    @Override
    public boolean isDisposed() {
        return this.events.isDisposed();
    }
}
