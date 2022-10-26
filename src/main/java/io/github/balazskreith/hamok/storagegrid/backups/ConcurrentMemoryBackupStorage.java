package io.github.balazskreith.hamok.storagegrid.backups;

import io.github.balazskreith.hamok.common.Utils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.StorageEndpoint;
import io.github.balazskreith.hamok.storagegrid.messages.DeleteEntriesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.storagegrid.messages.UpdateEntriesNotification;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrentMemoryBackupStorage<K, V> implements BackupStorage<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentMemoryBackupStorage.class);

    private final Subject<Set<K>> gaps = PublishSubject.create();
    private final StorageEndpoint<K, V> endpoint;
    private Map<UUID, Map<K, V>> storedEntries = new ConcurrentHashMap<>();
    private Map<K, SavedEntry<K>> savedEntries = new ConcurrentHashMap<>();
    private Map<K, V> bufferedEntries = new ConcurrentHashMap<>();
    private Random random = new Random();
    private AtomicReference<List<UUID>> remoteEndpointIdsListHolder = new AtomicReference<>(Collections.emptyList());
    private AtomicBoolean hasRemoteEndpoints = new AtomicBoolean(false);

    ConcurrentMemoryBackupStorage(StorageEndpoint<K, V> endpoint) {
        this.endpoint = endpoint.onUpdateEntriesNotification(notification -> {
            var remoteEndpointId = notification.sourceEndpointId();
            var remoteEndpointStoredEntries = this.storedEntries.get(remoteEndpointId);
            if (remoteEndpointStoredEntries == null) {
                remoteEndpointStoredEntries = new ConcurrentHashMap<>();
                this.storedEntries.put(remoteEndpointId, remoteEndpointStoredEntries);
            }
            remoteEndpointStoredEntries.putAll(notification.entries());
        }).onDeleteEntriesNotification(notification -> {
            this.evict(notification.sourceEndpointId(), notification.keys());
        }).onRemoteEndpointJoined(remoteEndpointId -> {
            var oldList = this.remoteEndpointIdsListHolder.get();
            var newList = Stream.concat(oldList.stream(), List.of(remoteEndpointId).stream())
                    .collect(Collectors.toList());
            this.setRemoteEndpoints(newList);
        }).onRemoteEndpointDetached(remoteEndpointId -> {
            var oldList = this.remoteEndpointIdsListHolder.get();
            var newList = oldList.stream()
                    .filter(key -> !UuidTools.equals(key, remoteEndpointId))
                    .collect(Collectors.toList());
            this.setRemoteEndpoints(newList);
            var lostBackups = this.savedEntries.values().stream()
                    .filter(savedEntry -> UuidTools.equals(remoteEndpointId, savedEntry.remoteEndpointId()))
                    .map(SavedEntry::key)
                    .collect(Collectors.toSet());
            if (0 < lostBackups.size()) {
                lostBackups.forEach(this.savedEntries::remove);
                this.gaps.onNext(lostBackups);
            }
        });
        var remoteEndpointsList = this.endpoint.getRemoteEndpointIds().stream().collect(Collectors.toList());
        this.setRemoteEndpoints(remoteEndpointsList);
//        logger.debug("Backup storage for {} is created", this.endpoint.getStorageId());
    }


    private void setRemoteEndpoints(List<UUID> remoteEndpointIds) {
        this.remoteEndpointIdsListHolder.set(remoteEndpointIds);
        if (hasRemoteEndpoints.get()) {
            var hasRemoteEndpoint = 0 < remoteEndpointIds.size();
            hasRemoteEndpoints.set(hasRemoteEndpoint);
        } else if (0 < remoteEndpointIds.size()) {
            // flush buffered entries
            hasRemoteEndpoints.set(true);
            this.save(this.bufferedEntries);
            this.bufferedEntries.clear();
        }
    }

    @Override
    public void receiveMessage(Message message) {
        if (message.protocol == message.protocol) {
            this.endpoint.receive(message);
        }
    }

    @Override
    public Observable<Set<K>> gaps() {
        return this.gaps;
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
            var savedEntry = this.savedEntries.remove(key);
            if (savedEntry == null) {
                this.bufferedEntries.remove(key);
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
            var notification =  new DeleteEntriesNotification<K>(null, remoteKeys, remoteEndpointId);
            this.endpoint.sendDeleteEntriesNotification(notification);
        });
    }


    @Override
    public void evict(UUID sourceId, Set<K> keys) {
        if (keys == null) {
            return;
        }
        this.storedEntries.forEach((endpointId, storedEntries) -> {
            if (sourceId != null && UuidTools.notEquals(endpointId, sourceId)) {
                return;
            }
            keys.stream().forEach(storedEntries::remove);
        });
    }

    @Override
    public Map<K, V> extract(UUID endpointId) {
        if (endpointId == null) {
            return Collections.emptyMap();
        }
        var result = Utils.firstNonNull(this.storedEntries.remove(endpointId), Collections.<K, V>emptyMap());
        logger.debug("{} Extracted {} entries for remote endpoint {}. Stored Entries: {}", this.endpoint.getLocalEndpointId(), result.size(), endpointId, this.storedEntries);
        return result;
    }

    @Override
    public void clear() {
        this.storedEntries.clear();
    }

    @Override
    public BackupStats metrics() {
        return  new BackupStats(
                "Backup",
                this.storedEntries.size(),
                this.savedEntries.size()
        );
    }

    private void saveEntries(UUID remoteEndpointId, Map<K, V> entries) {
        var notification = UpdateEntriesNotification.<K, V>builder()
                .setEntries(entries)
                .setDestinationEndpointId(remoteEndpointId)
                .build();
//        logger.info("{} Save entries {} on endpoint {}", this.endpoint.getLocalEndpointId(), JsonUtils.objectToString(entries), remoteEndpointId);
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
