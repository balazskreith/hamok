package io.github.balazskreith.vstorage.storagegrid.backups;

import io.github.balazskreith.vstorage.storagegrid.StorageEndpoint;
import io.github.balazskreith.vstorage.storagegrid.messages.EvictEntriesNotification;
import io.github.balazskreith.vstorage.storagegrid.messages.UpdateEntriesNotification;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrentMemoryBackupStorage<K, V> implements BackupStorage<K, V> {

    private final StorageEndpoint<K, V> endpoint;
    private Map<K, StoredEntry<K, V>> storedEntries = new ConcurrentHashMap<>();
    private Map<K, SavedEntry<K>> savedEntries = new ConcurrentHashMap<>();
    private Map<K, V> bufferedEntries = new ConcurrentHashMap<>();
    private Random random = new Random();
    private AtomicReference<List<UUID>> remoteEndpointIdsListHolder = new AtomicReference<>(Collections.emptyList());
    private AtomicBoolean hasRemoteEndpoints = new AtomicBoolean(false);

    ConcurrentMemoryBackupStorage(StorageEndpoint<K, V> endpoint) {
        this.endpoint = endpoint.onUpdateEntriesNotification(notification -> {
            var remoteEndpointId = notification.sourceEndpointId();
            var storedEntries = notification.entries().entrySet().stream().map(entry -> {
                return new StoredEntry<K, V>(entry.getKey(), entry.getValue(), remoteEndpointId);
            }).collect(Collectors.toMap(
                    entry -> entry.key(),
                    Function.identity()
            ));
            this.storedEntries.putAll(storedEntries);
        }).onDeleteEntriesNotification(notification -> {
            this.evict(notification.keys());
        }).onRemoteEndpointJoined(remoteEndpointId -> {
            var oldList = this.remoteEndpointIdsListHolder.get();
            var newList = Stream.concat(oldList.stream(), List.of(remoteEndpointId).stream())
                    .collect(Collectors.toList());
            this.setRemoteEndpoints(newList);
        }).onRemoteEndpointDetached(remoteEndpointId -> {
            var oldList = this.remoteEndpointIdsListHolder.get();
            var newList = oldList.stream()
                    .filter(key -> key != remoteEndpointId).collect(Collectors.toList());
            this.setRemoteEndpoints(newList);
        });
    }


    private void setRemoteEndpoints(List<UUID> remoteEndpointIds) {
        this.remoteEndpointIdsListHolder.set(remoteEndpointIds);
        if (hasRemoteEndpoints.get()) {
            hasRemoteEndpoints.set(0 < remoteEndpointIds.size());
        } else if (0 < remoteEndpointIds.size()) {
            // flush buffered entries
            hasRemoteEndpoints.set(true);
            this.save(this.bufferedEntries);
            this.bufferedEntries.clear();
        }
    }

    @Override
    public void save(Map<K, V> entries) {
        if (entries == null) {
            return;
        }
        if (hasRemoteEndpoints.get() == false) {
            this.bufferedEntries.putAll(entries);
            return;
        }
        var toUpdate = new HashMap<UUID, Map<K, V>>();
        var toCreate = new HashMap<K, V>();
        entries.forEach((key, value) -> {
            var savedEntry = this.savedEntries.get(key);
            if (savedEntry == null) {
                toCreate.put(key, value);
            } else {
                var remoteEndpointId = savedEntry.remoteEndpointId();
                var endpointEntries = toUpdate.get(remoteEndpointId);
                if (endpointEntries == null) {
                    endpointEntries = new HashMap<>();
                    toUpdate.put(remoteEndpointId, endpointEntries);
                }
                endpointEntries.put(key, value);
            }
        });
        if (0 < toCreate.size()) {
            var remoteEndpointId = this.getRandomRemoteEndpointId();
            this.saveEntries(remoteEndpointId, toCreate);
        }

        if (0 < toUpdate.size()) {
            toUpdate.forEach((remoteEndpointId, updatedEntries) -> {
                this.saveEntries(remoteEndpointId, updatedEntries);
            });
        }
    }

    @Override
    public void delete(Set<K> keys) {
        if (keys == null) {
            return;
        }
        var toDelete = new HashMap<UUID, Set<K>>();
        keys.forEach(key -> {
            var storedEntry = this.storedEntries.remove(key);
            if (storedEntry != null) {
                return;
            }
            var savedEntry = this.savedEntries.get(key);
            if (savedEntry == null) {
                return;
            }
            var remoteEndpointId = savedEntry.remoteEndpointId();
            var remoteKeys = toDelete.get(remoteEndpointId);
            if (remoteKeys == null) {
                remoteKeys = new HashSet<>();
                toDelete.put(remoteEndpointId, remoteKeys);
            }
            remoteKeys.add(key);
        });
        if (toDelete.size() < 1) {
            return;
        }
        toDelete.forEach((remoteEndpointId, remoteKeys) -> {
            var notification =  EvictEntriesNotification.<K>builder()
                    .setDestinationEndpointId(remoteEndpointId)
                    .setKeys(remoteKeys)
                    .build();
            this.endpoint.sendEvictedEntriesNotification(notification);
        });
    }

    @Override
    public void evict(Set<K> keys) {
        if (keys == null) {
            return;
        }
        keys.stream().filter(key -> this.bufferedEntries.remove(key) == null)
                .forEach(this.storedEntries::remove);
    }

    @Override
    public Map<K, V> extract(UUID endpointId) {
        if (endpointId == null) {
            return Collections.emptyMap();
        }
        var keys = this.storedEntries.values().stream()
                .filter(entry -> entry.remoteEndpointId() == endpointId)
                .collect(Collectors.toSet());
        var result = keys.stream().map(this.storedEntries::remove)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        entry -> entry.key(),
                        entry -> entry.value()
                ));
        return result;
    }

    @Override
    public void clear() {
        this.storedEntries.clear();
    }

    private void saveEntries(UUID remoteEndpointId, Map<K, V> entries) {
        var notification = UpdateEntriesNotification.<K, V>builder()
                .setEntries(entries)
                .setDestinationEndpointId(remoteEndpointId)
                .build();
        this.endpoint.sendUpdateEntriesNotification(notification);
        var toSave = entries.keySet().stream().map(key -> new SavedEntry<K>(key, remoteEndpointId)).collect(Collectors.toMap(
                savedEntry -> savedEntry.key(),
                Function.identity()
        ));
        this.savedEntries.putAll(toSave);
    }

    private UUID getRandomRemoteEndpointId() {
        var remoteEndpointIds = remoteEndpointIdsListHolder.get();
        if (remoteEndpointIds == null || remoteEndpointIds.size() < 1) {
            return null;
        }

        int randomElement = this.random.nextInt(remoteEndpointIds.size());
        return remoteEndpointIds.get(randomElement);
    }

    @Override
    public void close() throws Exception {

    }
}
