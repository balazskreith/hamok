package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.storagegrid.messages.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StorageEndpoint<K, V> implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(StorageEndpoint.class);

    // assigned by the grid
    StorageGrid grid;
    Supplier<Depot<Map<K, V>>> depotProvider;
    Supplier<Set<UUID>> defaultResolvingEndpointIds;

    StorageOpSerDe<K, V> messageSerDe;
    String storageId;
    String protocol;

    final Disposer disposer;
    // created by the constructor
    private final Map<UUID, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final Subject<Message> requestsDispatcherSubject = PublishSubject.create();

    private final Subject<Message> getEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> deleteEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> insertEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> updateEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> insertEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> updateEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> deleteEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> getSizeRequestSubject = PublishSubject.create();
    private final Subject<Message> getKeysRequestSubject = PublishSubject.create();
    private final Subject<Message> clearEntriesNotificationSubject = PublishSubject.create();

    StorageEndpoint() {
        this.disposer = Disposer.builder()
                .addDisposable(Disposable.fromRunnable(() -> {
                    this.pendingRequests.values().forEach(p -> {
                        try {
                            p.cancel(true);
                        } catch(Exception ex) {
                            logger.warn("Exception occurred while cancelling request", ex);
                        }
                    });
                }))
                .addSubject(this.getEntriesRequestSubject)
                .addSubject(this.deleteEntriesRequestSubject)
                .addSubject(this.updateEntriesRequestSubject)
                .addSubject(this.deleteEntriesNotificationSubject)
                .addSubject(this.updateEntriesRequestSubject)
                .addSubject(this.insertEntriesRequestSubject)
                .addSubject(this.getSizeRequestSubject)
                .addSubject(this.getKeysRequestSubject)
                .addSubject(this.clearEntriesNotificationSubject)
                .onCompleted(() -> {
                    logger.info("Disposed. StorageId: {}, protocol: {}", this.storageId, this.protocol);
                })
                .build();
    }

    void init() {
        this.disposer.add(this.grid.detachedRemoteEndpoints().subscribe(detachedEndpointId -> {
            for (var pendingRequest : this.pendingRequests.values()) {
                pendingRequest.removeEndpointId(detachedEndpointId);
            }
        }));
    }

    public String getStorageId() {
        return this.storageId;
    }

    public Set<UUID> getRemoteEndpointIds() {
        return this.grid.getRemoteEndpointIds();
    }

    public UUID getLocalEndpointId() {
        return this.grid.getLocalEndpointId();
    }

    void receive(Message message) {
        if (message.protocol != null && !message.protocol.equals(this.protocol)) {
            logger.debug("Ignore received message {}, message protocol: {}, endpoint protocol: {}", message.type, message.protocol, this.protocol);
            // this is not for this endpoint
            return;
        }
//        logger.info("Message is received from {} type {}, protocol {}", message.sourceId, message.type, message.protocol);
        var type = MessageType.valueOf(message.type);
        switch (type) {
            case GET_ENTRIES_REQUEST -> this.getEntriesRequestSubject.onNext(message);
            case UPDATE_ENTRIES_REQUEST -> this.updateEntriesRequestSubject.onNext(message);
            case UPDATE_ENTRIES_NOTIFICATION -> this.updateEntriesNotificationSubject.onNext(message);
            case INSERT_ENTRIES_REQUEST -> this.insertEntriesRequestSubject.onNext(message);
            case INSERT_ENTRIES_NOTIFICATION -> this.insertEntriesNotificationSubject.onNext(message);
            case DELETE_ENTRIES_REQUEST -> this.deleteEntriesRequestSubject.onNext(message);
            case DELETE_ENTRIES_NOTIFICATION -> this.deleteEntriesNotificationSubject.onNext(message);
            case GET_SIZE_REQUEST -> this.getSizeRequestSubject.onNext(message);
            case GET_KEYS_REQUEST -> this.getKeysRequestSubject.onNext(message);
            case CLEAR_ENTRIES_NOTIFICATION -> this.clearEntriesNotificationSubject.onNext(message);
            case DELETE_ENTRIES_RESPONSE,
                    GET_ENTRIES_RESPONSE,
                    GET_SIZE_RESPONSE,
                    GET_KEYS_RESPONSE,
                    INSERT_ENTRIES_RESPONSE,
                    UPDATE_ENTRIES_RESPONSE -> this.processResponse(message);
            default -> {
                logger.warn("Message type is not recognized {} ", JsonUtils.objectToString(message));
            }
        }
    }

    Observable<Message> requestsDispatcher() {
        return this.requestsDispatcherSubject.map(message -> {
            message.protocol = this.protocol;
            message.storageId = this.storageId;
            return message;
        });
    }

    public StorageEndpoint<K, V> onDeleteEntriesRequest(Consumer<DeleteEntriesRequest<K>> listener) {
        this.disposer.add(this.deleteEntriesRequestSubject
                .map(this.messageSerDe::deserializeDeleteEntriesRequest)
                .subscribe(listener)
        );
        return this;
    }

    public StorageEndpoint<K, V> onRemoteEndpointJoined(Consumer<UUID> listener) {
        this.disposer.add(this.grid.joinedRemoteEndpoints().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onRemoteEndpointDetached(Consumer<UUID> listener) {
        this.disposer.add(this.grid.detachedRemoteEndpoints().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onLocalEndpointReset(Consumer<Long> listener) {
        this.disposer.add(this.grid.inactivatedLocalEndpoint().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onLeaderIdChanged(Consumer<Optional<UUID>> listener) {
        this.disposer.add(this.grid.changedLeaderId().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onDeleteEntriesNotification(Consumer<DeleteEntriesNotification<K>> listener) {
        this.deleteEntriesNotificationSubject
                .map(this.messageSerDe::deserializeDeleteEntriesNotification)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onUpdateEntriesRequest(Consumer<UpdateEntriesRequest<K, V>> listener) {
        this.updateEntriesRequestSubject
                .map(this.messageSerDe::deserializeUpdateEntriesRequest)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onInsertEntriesRequest(Consumer<InsertEntriesRequest<K, V>> listener) {
        this.insertEntriesRequestSubject
                .map(this.messageSerDe::deserializeInsertEntriesRequest)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onInsertEntriesNotification(Consumer<InsertEntriesNotification<K, V>> listener) {
        this.insertEntriesNotificationSubject
                .map(this.messageSerDe::deserializeInsertEntriesNotification)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onUpdateEntriesNotification(Consumer<UpdateEntriesNotification<K, V>> listener) {
        this.updateEntriesNotificationSubject
                .map(this.messageSerDe::deserializeUpdateEntriesNotification)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onGetEntriesRequest(Consumer<GetEntriesRequest<K>> listener) {
        this.getEntriesRequestSubject
                .map(this.messageSerDe::deserializeGetEntriesRequest)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onGetSizeRequest(Consumer<GetSizeRequest> listener) {
        this.getSizeRequestSubject
                .map(this.messageSerDe::deserializeGetSizeRequest)
                .subscribe(listener);
        return this;
    }

    public StorageEndpoint<K, V> onClearEntriesNotification(Consumer<ClearEntriesNotification> listener) {
        this.clearEntriesNotificationSubject
                .map(this.messageSerDe::deserializeClearEntriesNotification)
                .subscribe(listener);
        return this;
    }

    public void sendClearEntriesNotification(ClearEntriesNotification notification) {
        var message = this.messageSerDe.serializeClearEntriesNotification(notification);
        this.send(message);
    }

    public int requestGetSize() {
        var request = new GetSizeRequest(UUID.randomUUID(), null);
        var message = this.messageSerDe.serializeGetSizeRequest(request);
        var sum = this.request(message).stream()
                .map(this.messageSerDe::deserializeGetSizeResponse)
                .map(GetSizeResponse::size)
                .reduce((r, i) -> r + i);
        if (sum.isEmpty()) {
            return 0;
        }
        return sum.get();
    }

    public void sendGetSizeResponse(GetSizeResponse response) {
        var message = this.messageSerDe.serializeGetSizeResponse(response);
        this.send(message);
    }

    public StorageEndpoint<K, V> onGetKeysRequest(Consumer<GetKeysRequest> listener) {
        this.getKeysRequestSubject
                .map(this.messageSerDe::deserializeGetKeysRequest)
                .subscribe(listener);
        return this;
    }

    public Set<K> requestGetKeys() {
        var request = new GetKeysRequest(UUID.randomUUID(), null);
        var message = this.messageSerDe.serializeGetKeysRequest(request);
        return this.request(message).stream()
                .map(this.messageSerDe::deserializeGetKeysResponse)
                .map(GetKeysResponse::keys)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public void sendGetKeysResponse(GetKeysResponse<K> response) {
        var message = this.messageSerDe.serializeGetKeysResponse(response);
        this.send(message);
    }

    public Map<K, V> requestGetEntries(Set<K> keys) {
        var request = GetEntriesRequest.<K>builder()
                .setKeys(keys)
                .build();
        var message = this.messageSerDe.serializeGetEntriesRequest(request);
        var depot = depotProvider.get();
        var responses = this.request(message);
        responses.stream()
                .map(this.messageSerDe::deserializeGetEntriesResponse)
                .map(GetEntriesResponse::foundEntries)
                .forEach(depot::accept);
        var result = depot.get();
        logger.debug("{} collected responses: {}, depot: {}", this.getLocalEndpointId(), JsonUtils.objectToString(responses), JsonUtils.objectToString(depot.get()));
        return result;
    }

    public void sendGetEntriesResponse(GetEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeGetEntriesResponse(response);
        logger.debug("{} sending {} response to {}", this.grid.getLocalEndpointId().toString().substring(0, 8), message.type, message.destinationId);
        this.send(message);
    }

    public Set<K> requestDeleteEntries(Set<K> keys) {
        var request = DeleteEntriesRequest.<K>builder()
                .setKeys(keys)
                .build();
        var message = this.messageSerDe.serializeDeleteEntriesRequest(request);
        return this.request(message).stream()
                .map(this.messageSerDe::deserializeDeleteEntriesResponse)
                .map(DeleteEntriesResponse::deletedKeys)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public void sendDeleteEntriesNotification(DeleteEntriesNotification<K> notification) {
        var message = this.messageSerDe.serializeDeleteEntriesNotification(notification);
        this.send(message);
    }

    public void sendDeleteEntriesResponse(DeleteEntriesResponse<K> response) {
        var message = this.messageSerDe.serializeDeleteEntriesResponse(response);
        this.send(message);
    }

    public void sendUpdateEntriesNotification(UpdateEntriesNotification<K, V> notification) {
        var message = this.messageSerDe.serializeUpdateEntriesNotification(notification);
        this.send(message);
    }

    public Map<K, V> requestUpdateEntries(Map<K, V> entries) {
        var request = UpdateEntriesRequest.<K, V>builder()
                .setEntries(entries)
                .build();
        var message = this.messageSerDe.serializeUpdateEntriesRequest(request);
        var depot = depotProvider.get();
        this.request(message).stream()
                .map(this.messageSerDe::deserializeUpdateEntriesResponse)
                .map(UpdateEntriesResponse::entries)
                .forEach(depot::accept);
        return depot.get();
    }

    public Map<K, V> requestInsertEntries(Map<K, V> entries) {
        var request = new InsertEntriesRequest(UUID.randomUUID(), entries, this.grid.getLocalEndpointId());
        var message = this.messageSerDe.serializeInsertEntriesRequest(request);
        var depot = depotProvider.get();
        this.request(message).stream()
                .map(this.messageSerDe::deserializeInsertEntriesResponse)
                .map(InsertEntriesResponse::existingEntries)
                .forEach(depot::accept);
        return depot.get();
    }

    public void sendInsertEntriesNotification(InsertEntriesNotification<K, V> notification) {
        var message = this.messageSerDe.serializeInsertEntriesNotification(notification);
        this.send(message);
    }

    public void sendInsertEntriesResponse(InsertEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeInsertEntriesResponse(response);
        this.send(message);
    }

    public void sendUpdateEntriesResponse(UpdateEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeUpdateEntriesResponse(response);
        this.send(message);
    }

    private void processResponse(Message message) {
        if (message.requestId == null) {
            logger.warn("RequestId is null in response {}", JsonUtils.objectToString(message));
            return;
        }
        logger.debug("{} Receiving message for request id {} for type: {}", this.grid.getLocalEndpointId(), message.requestId, message.type);
        var pendingRequest = this.pendingRequests.get(message.requestId);
        if (pendingRequest == null) {
            logger.warn("{} No pending request found for message {}", this.grid.getLocalEndpointId() ,JsonUtils.objectToString(message));
            return;
        }
        logger.debug("{} Receiving response for request id {} for type: {}, {}",this.grid.getLocalEndpointId(), message.requestId, message.type, pendingRequest);
        pendingRequest.accept(message);
    }

    private List<Message> request(Message message) {
        return this.request(message, 0);
    }

    private List<Message> request(Message message, int retried) {
        if (3 < retried) {
            logger.error("Cannot resolve request", JsonUtils.objectToString(message));
            return Collections.emptyList();
        }
        var requestId = message.requestId;
        Set<UUID> remoteEndpointIds;
        if (message.destinationId != null) {
            remoteEndpointIds = Set.of(message.destinationId);
        } else {
            remoteEndpointIds = this.defaultResolvingEndpointIds.get();
        }
        logger.info("Creating request at {} ({}) remote endpoints: {}", this.grid.getLocalEndpointId(), this.grid.getContext(), JsonUtils.objectToString(remoteEndpointIds));
        if (remoteEndpointIds.size() < 1) {
            return Collections.emptyList();
        }
        var pendingRequest = PendingRequest.builder()
                .withRequestId(requestId)
                .withPendingEndpoints(remoteEndpointIds)
                .withTimeoutInMs(this.grid.getRequestTimeoutInMs() * (retried + 1))
                .build();

        this.pendingRequests.put(requestId, pendingRequest);

        logger.debug("{} Sending request (type: {}, id: {})", this.grid.getLocalEndpointId().toString().substring(0, 8), message.type, requestId);
        this.requestsDispatcherSubject.onNext(message);

        try {
            var result =  pendingRequest.get();
            this.pendingRequests.remove(requestId);
            return result;
        } catch (ExecutionException e) {
            logger.warn("Error occurred while processing request {} ", requestId , e);
            this.pendingRequests.remove(requestId);
            message.requestId = UUID.randomUUID();
            return this.request(message, retried + 1);
        } catch (InterruptedException e) {
            logger.warn("Error occurred while processing request {} ", requestId, e);
            this.pendingRequests.remove(requestId);
            message.requestId = UUID.randomUUID();
            return this.request(message, retried + 1);
        } catch (TimeoutException e) {
            logger.warn("Timeout occurred while processing request {} ", requestId, e);
            this.pendingRequests.remove(requestId);
            message.requestId = UUID.randomUUID();
            return this.request(message, retried + 1);
        }
    }

    private void send(Message message) {
        message.storageId = this.storageId;
        message.protocol = this.protocol;
        this.grid.send(message);
    }

    @Override
    public void dispose() {
        this.disposer.dispose();
    }

    @Override
    public boolean isDisposed() {
        return this.disposer.isDisposed();
    }
}
