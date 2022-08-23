package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.rxutils.RxCollector;
import io.github.balazskreith.hamok.storagegrid.backups.DistributedBackups;
import io.github.balazskreith.hamok.storagegrid.messages.DeleteEntriesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.MessageType;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import io.github.balazskreith.hamok.storagegrid.messages.UpdateEntriesNotification;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InMemoryDistributedBackups implements DistributedBackups {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryDistributedBackups.class);

    public static Builder builder() {
        return new Builder();
    }

    record StoredEntry(String value, UUID sourceEndpointId) {

    }

    enum Event {
        ADD,
        REMOVE
    }
    record BufferedEvent(
            Event type,
            String key,
            String value
    ) {

    }

    private String id;
    private StorageEndpoint<String, String> endpoint;
    private Map<String, StoredEntry> storedEntries = new ConcurrentHashMap<>();
    private Map<String, UUID> savedEntries = new ConcurrentHashMap<>();
    private Random random = new Random();
    private AtomicReference<List<UUID>> remoteEndpointIdsListHolder = new AtomicReference<>(Collections.emptyList());
    private RxCollector<BufferedEvent> events;
    private final Subject<String> onClosedSubject = PublishSubject.create();

    private InMemoryDistributedBackups() {

    }

    private void init() {
//        this.endpoint.getRemoteEndpointIds().
        this.endpoint
                .onRemoveEntriesRequest(request -> {
                    var sourceEndpointId = request.sourceEndpointId();
                    var removedEntries = new HashMap<String, String>();
                    for (var entry : this.storedEntries.entrySet()) {
                        var key = entry.getKey();
                        var storedEntry = entry.getValue();
                        if (!request.keys().contains(key)) {
                            continue;
                        }
                        if (UuidTools.notEquals(storedEntry.sourceEndpointId, sourceEndpointId)) {
                            continue;
                        }
                        removedEntries.put(key, storedEntry.value);
                    }
                    var response = request.createResponse(removedEntries);
                    this.endpoint.sendRemoveEntriesResponse(response);
                })
                .onUpdateEntriesNotification(notification -> {
                    var sourceEndpointId = notification.sourceEndpointId();
                    for (var entry : notification.entries().entrySet()) {
                        var key = entry.getKey();
                        var value = entry.getValue();
                        this.store(key, value, sourceEndpointId);
                    }
                })
                .onDeleteEntriesNotification(notification -> {
                    for (var key : notification.keys()) {
                        var storedEntry = this.storedEntries.remove(key);
                        if (storedEntry == null) {
                            logger.debug("Ordered to remove an entry has not been stored. request origin endpoint id: {}", notification.sourceEndpointId());
                        }
                    }
                })
                .onRemoteEndpointJoined(remoteEndpointId -> {
                    var oldList = this.remoteEndpointIdsListHolder.get();
                    var newList = Stream.concat(oldList.stream(), List.of(remoteEndpointId).stream())
                            .collect(Collectors.toList());
                    this.remoteEndpointIdsListHolder.set(newList);
                })
                .onRemoteEndpointDetached(remoteEndpointId -> {
                    var oldList = this.remoteEndpointIdsListHolder.get();
                    var newList = oldList.stream()
                            .filter(key -> !UuidTools.equals(key, remoteEndpointId))
                            .collect(Collectors.toList());
                    this.remoteEndpointIdsListHolder.set(newList);
                });
        var remoteEndpointsList = this.endpoint.getRemoteEndpointIds().stream().collect(Collectors.toList());
        this.remoteEndpointIdsListHolder.set(remoteEndpointsList);
        this.events.subscribe(events -> {
            var deletedKeys = new HashSet<String>();
            var addedEntries = new HashMap<String, String>();
            for (var event : events) {
                switch (event.type) {
                    case ADD -> {
                        addedEntries.put(event.key, event.value);
                        deletedKeys.remove(event.key);
                    }
                    case REMOVE -> {
                        addedEntries.remove(event.key);
                        deletedKeys.add(event.key);
                    }
                }
            }
            if (0 < deletedKeys.size()) {
                this.sendDelete(deletedKeys);
            }
            if (0 < addedEntries.size()) {
                this.sendSaveEntries(addedEntries);
            }
        });
    }

    public int getStoredEntriesSize() {
        return this.storedEntries.size();
    }

    public int getSavedEntriesSize() {
        return this.savedEntries.size();
    }

    public int getStoredEntriesBytesLength() {
        return this.storedEntries.values().stream()
                .map(entry -> entry.value.getBytes().length)
                .reduce((acc, v) -> acc + v)
                .orElse(0);
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void save(String key, String value) {
        this.events.onNext(new BufferedEvent(
                Event.ADD,
                key,
                value
        ));
    }

    @Override
    public Map<String, Optional<String>> loadFrom(UUID destinationEndpointId) {
        var savedKeys = new HashSet<String>();
        for (var it = this.savedEntries.entrySet().iterator(); it.hasNext(); ) {
            var savedEntry = it.next();
            var key = savedEntry.getKey();
            var savedEndpointId = savedEntry.getValue();
            if (UuidTools.notEquals(savedEndpointId, destinationEndpointId)) {
                continue;
            }
            savedKeys.add(key);
            it.remove();
        }
        if (savedKeys.size() < 1) {
            return Collections.emptyMap();
        }
        if (!this.endpoint.getRemoteEndpointIds().contains(destinationEndpointId)) {
            return savedKeys.stream().collect(Collectors.toMap(
                    Function.identity(),
                    key -> Optional.empty()
            ));
        }
        var removedEntries = this.endpoint.requestRemoveEntries(savedKeys, Set.of(destinationEndpointId));
        return savedKeys.stream().collect(Collectors.toMap(
                Function.identity(),
                key -> removedEntries.containsKey(key) ? Optional.of(removedEntries.get(key)) : Optional.empty()
        ));
    }

    @Override
    public void delete(String key) {
        this.events.onNext(new BufferedEvent(
                Event.REMOVE,
                key,
                null
        ));
    }

    @Override
    public void store(String key, String value, UUID sourceEndpointId) {
        var prevStoredEntry = this.storedEntries.put(key, new StoredEntry(
                value,
                sourceEndpointId
        ));
        if (prevStoredEntry != null && UuidTools.notEquals(prevStoredEntry.sourceEndpointId, sourceEndpointId)) {
            logger.warn("Backup for key {} received from a different endpoint then it saved previously. original source endpoint id: {}, new source endpoint id {}",
                    key,
                    prevStoredEntry.sourceEndpointId,
                    sourceEndpointId
            );
        }
    }


    @Override
    public Map<String, String> extract(UUID endpointId) {
        var result = new HashMap<String, String>();
        for (var it = this.storedEntries.entrySet().iterator(); it.hasNext(); it.hasNext() ) {
            var entry = it.next();
            var key = entry.getKey();
            var storedEntry = entry.getValue();
            if (UuidTools.notEquals(endpointId, storedEntry.sourceEndpointId)) {
                continue;
            }
            result.put(key, storedEntry.value());
            it.remove();
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public void evict(Set<String> keys, UUID endpointId) {
        for (var it = this.storedEntries.entrySet().iterator(); it.hasNext(); it.hasNext() ) {
            var entry = it.next();
            var key = entry.getKey();
            var storedEntry = entry.getValue();
            if (endpointId != null && UuidTools.notEquals(endpointId, storedEntry.sourceEndpointId)) {
                continue;
            }
            if (!keys.contains(key)) {
                continue;
            }
            it.remove();
        }
    }

    private void sendSaveEntries(Map<String, String> entries) {
        if (entries == null || entries.size() < 1) {
            return;
        }
        var selectedRemoteEndpointId = this.getRandomRemoteEndpointId();
        var endpointEntries = new HashMap<UUID, Map<String, String>>();
        for (var entry : entries.entrySet()) {
            var key = entry.getKey();
            var value = entry.getValue();
            var destinationEndpointId = this.savedEntries.getOrDefault(key, selectedRemoteEndpointId);
            var toUpdate = endpointEntries.get(destinationEndpointId);
            if (toUpdate == null) {
                toUpdate = new HashMap<>();
                endpointEntries.put(destinationEndpointId, toUpdate);
            }
            toUpdate.put(key, value);
        }

        for (var entry : endpointEntries.entrySet()) {
            var destinationEndpointId = entry.getKey();
            var updatedEntries = entry.getValue();
            this.endpoint.sendUpdateEntriesNotification(new UpdateEntriesNotification<>(
                    updatedEntries,
                    this.endpoint.getLocalEndpointId(),
                    destinationEndpointId
            ));
            for (var updatedKey : updatedEntries.keySet()) {
                var prevDestinationId = this.savedEntries.put(updatedKey, destinationEndpointId);
                if (prevDestinationId != null && UuidTools.notEquals(destinationEndpointId, prevDestinationId)) {
                    logger.warn("Value for key {} is previously saved on {}, but a new update is sent to {}. Might cause inconsistency",
                            updatedKey,
                            prevDestinationId,
                            destinationEndpointId
                    );
                }
            }
        }
    }

    private void sendDelete(Set<String> keys) {
        if (keys == null || keys.size() < 1) {
            return;
        }
        Map<UUID, Set<String>> endpointToDeleteKeys = new HashMap<>();
        for (var key : keys) {
            var endpointId = this.savedEntries.remove(key);
            if (endpointId == null) {
                logger.warn("Cannot delete backup entry for key {}, because there is no remote endpoint it is saved", key);
                continue;
            }
            var toDelete = endpointToDeleteKeys.get(endpointId);
            if (toDelete == null) {
                toDelete = new HashSet<>();
                endpointToDeleteKeys.put(endpointId, toDelete);
            }
            toDelete.add(key);
        }
        for (var entry : endpointToDeleteKeys.entrySet()) {
            var endpointId = entry.getKey();
            var toDelete = entry.getValue();
            this.endpoint.sendDeleteEntriesNotification(new DeleteEntriesNotification<>(
                    this.endpoint.getLocalEndpointId(),
                    toDelete,
                    endpointId
            ));
        }
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
        this.events.onComplete();
        this.endpoint.close();
        this.savedEntries.clear();
        this.storedEntries.clear();
        this.onClosedSubject.onNext(this.id);
    }

    public static class Builder {
        private int maxEvents = 1000;
        private int maxTimeInMs = 1000;
        private StorageGrid grid;
        private InMemoryDistributedBackups result = new InMemoryDistributedBackups();
        private boolean throwExceptionOnRequestTimeout = true;

        private Builder() {

        }

        public Builder setId(String id) {
            this.result.id = id;
            return this;
        }

        public Builder setGrid(StorageGrid grid) {
            this.grid = grid;
            return this;
        }

        public Builder setMaxCollectingEvents(int value) {
            this.maxEvents = value;
            return this;
        }

        public Builder setMaxEventCollectingTimeInMs(int value) {
            this.maxTimeInMs = value;
            return this;
        }

        public Builder setThrowingExceptionOnRequestTimeout(boolean value) {
            this.throwExceptionOnRequestTimeout = value;
            return this;
        }

        public InMemoryDistributedBackups build() {
            Objects.requireNonNull(this.grid, "Grid must be set to build in memory distributed backups");
            Objects.requireNonNull(this.result.id, "Cannot create an in memory distributed backups without id");
            var serDe = new StorageOpSerDe<String, String>(
                    String::getBytes,
                    bytes -> new String(bytes),
                    String::getBytes,
                    bytes -> new String(bytes)
            );
            var storageEndpointConfig = new StorageEndpointConfig(
                    DistributedBackups.PROTOCOL_NAME,
                    this.throwExceptionOnRequestTimeout
            );
            this.result.endpoint = new StorageEndpoint<String, String>(
                    this.grid,
                    serDe,
                    new ResponseMessageChunker(this.maxEvents, this.maxEvents),
                    MapUtils::makeMapAssignerDepot,
                    storageEndpointConfig
            ) {
                @Override
                protected String getStorageId() {
                    return result.id;
                }

                @Override
                protected Set<UUID> defaultResolvingEndpointIds(MessageType messageType) {
                    return grid.endpoints().getRemoteEndpointIds();
                }

                @Override
                protected void sendNotification(Models.Message.Builder message) {
                    grid.send(message);
                }

                @Override
                protected void sendRequest(Models.Message.Builder message) {
                    grid.send(message);
                }

                @Override
                protected void sendResponse(Models.Message.Builder message) {
                    grid.send(message);
                }
            };
            this.grid.addStorageInGrid(new StorageInGrid() {

                @Override
                public String getIdentifier() {
                    return result.id;
                }

                @Override
                public void accept(Models.Message message) {
                    result.endpoint.receive(message);
                }

                @Override
                public void close() {
                    try {
                        result.close();
                    } catch (Exception e) {
                        logger.warn("Error occurred while closing resource", e);
                    }
                }

                @Override
                public StorageSyncResult executeSync() {
                    return new StorageSyncResult(
                            true,
                            Collections.emptyList()
                    );
                }

                @Override
                public Observable<String> observableClosed() {
                    return result.onClosedSubject;
                }

                @Override
                public StorageEndpoint.Stats storageEndpointStats() {
                    return result.endpoint.metrics();
                }
            });
            this.result.events = RxCollector.<BufferedEvent>builder()
                    .withMaxTimeInMs(this.maxTimeInMs)
                    .withMaxItems(this.maxEvents)
                    .build();
            this.result.init();
            return result;
        }
    }
}
