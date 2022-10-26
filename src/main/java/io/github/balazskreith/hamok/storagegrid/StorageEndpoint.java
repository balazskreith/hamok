package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.FailedOperationException;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.*;
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
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class StorageEndpoint<K, V> implements AutoCloseable {
    private static final Integer ZERO = 0;
    private static final Logger logger = LoggerFactory.getLogger(StorageEndpoint.class);

    // assigned by the grid
    private final StorageGrid grid;
    private final Function<Message, Iterator<Message>> responseMessageChunker;
    private final StorageOpSerDe<K, V> messageSerDe;
    private final String protocol;
    private final Supplier<Depot<Map<K, V>>> depotProvider;

    final Disposer disposer;
    // created by the constructor
    private final Map<UUID, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final Map<UUID, PendingResponse> pendingResponses = new ConcurrentHashMap<>();

    private final Subject<Message> clearEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> getEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> deleteEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> insertEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> updateEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> insertEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> updateEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> deleteEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> removeEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> getSizeRequestSubject = PublishSubject.create();
    private final Subject<Message> getKeysRequestSubject = PublishSubject.create();
    private final Subject<Message> clearEntriesNotificationSubject = PublishSubject.create();



    StorageEndpoint(
            StorageGrid grid,
            StorageOpSerDe<K, V> messageSerDe,
            Function<Message, Iterator<Message>> responseMessageChunker,
            Supplier<Depot<Map<K, V>>> depotProvider,
            String protocol
    ) {
        this.grid = grid;
        this.depotProvider = depotProvider;
        this.messageSerDe = messageSerDe;
        this.responseMessageChunker = responseMessageChunker;
        this.protocol = protocol;
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
                .addSubject(this.clearEntriesRequestSubject)
                .addSubject(this.getEntriesRequestSubject)
                .addSubject(this.deleteEntriesRequestSubject)
                .addSubject(this.updateEntriesRequestSubject)
                .addSubject(this.deleteEntriesNotificationSubject)
                .addSubject(this.updateEntriesRequestSubject)
                .addSubject(this.insertEntriesRequestSubject)
                .addSubject(this.removeEntriesNotificationSubject)
                .addSubject(this.getSizeRequestSubject)
                .addSubject(this.getKeysRequestSubject)
                .addSubject(this.clearEntriesNotificationSubject)
                .onCompleted(() -> {
                    logger.info("Disposed endpoint for storage: {}, protocol: {}", this.getStorageId(), this.protocol);
                })
                .build();
    }

    void init() {
        this.disposer.add(this.grid.events().detachedRemoteEndpoints().subscribe(detachedEndpointId -> {
            for (var pendingRequest : this.pendingRequests.values()) {
//                logger.warn("Removing {} endpoint from pending Request: {}", detachedEndpointId, pendingRequest);
                pendingRequest.removeEndpointId(detachedEndpointId);
            }
        }));
    }

    protected abstract String getStorageId();

    public Set<UUID> getRemoteEndpointIds() {
        return this.grid.endpoints().getRemoteEndpointIds();
    }

    public UUID getLocalEndpointId() {
        return this.grid.endpoints().getLocalEndpointId();
    }

    public void receive(Message message) {
        if (message.protocol != null && !message.protocol.equals(this.protocol)) {
            logger.debug("Ignore received message {}, message protocol: {}, endpoint protocol: {}", message.type, message.protocol, this.protocol);
            // this is not for this endpoint
            return;
        }
//        logger.info("Message is received from {} type {}, protocol {}", message.sourceId, message.type, message.protocol);
        var type = MessageType.valueOf(message.type);
        switch (type) {
            case CLEAR_ENTRIES_REQUEST -> this.clearEntriesRequestSubject.onNext(message);
            case GET_ENTRIES_REQUEST -> this.getEntriesRequestSubject.onNext(message);
            case GET_SIZE_REQUEST -> this.getSizeRequestSubject.onNext(message);
            case GET_KEYS_REQUEST -> this.getKeysRequestSubject.onNext(message);
            case UPDATE_ENTRIES_REQUEST -> this.updateEntriesRequestSubject.onNext(message);
            case INSERT_ENTRIES_REQUEST -> this.insertEntriesRequestSubject.onNext(message);
            case DELETE_ENTRIES_REQUEST -> this.deleteEntriesRequestSubject.onNext(message);
            case UPDATE_ENTRIES_NOTIFICATION -> this.updateEntriesNotificationSubject.onNext(message);
            case INSERT_ENTRIES_NOTIFICATION -> this.insertEntriesNotificationSubject.onNext(message);
            case DELETE_ENTRIES_NOTIFICATION -> this.deleteEntriesNotificationSubject.onNext(message);
            case CLEAR_ENTRIES_NOTIFICATION -> this.clearEntriesNotificationSubject.onNext(message);
            case REMOVE_ENTRIES_NOTIFICATION -> this.removeEntriesNotificationSubject.onNext(message);
            case CLEAR_ENTRIES_RESPONSE,
                    DELETE_ENTRIES_RESPONSE,
                    GET_ENTRIES_RESPONSE,
                    GET_SIZE_RESPONSE,
                    GET_KEYS_RESPONSE,
                    INSERT_ENTRIES_RESPONSE,
                    UPDATE_ENTRIES_RESPONSE -> this.processResponse(message);
            default -> {
                logger.warn("Message type is not recognized {} ", message);
            }
        }
    }

    UUID getLeaderId() {
        return this.grid.endpoints().getLeaderEndpointId();
    }

    boolean isLeaderEndpoint() {
        return UuidTools.equals(this.grid.endpoints().getLeaderEndpointId(), this.grid.endpoints().getLocalEndpointId());
    }

    public StorageEndpoint<K, V> onClearEntriesRequest(Consumer<ClearEntriesRequest> listener) {
        this.disposer.add(this.clearEntriesRequestSubject
                .map(this.messageSerDe::deserializeClearEntriesRequest)
                .subscribe(listener)
        );
        return this;
    }


    public StorageEndpoint<K, V> onDeleteEntriesRequest(Consumer<DeleteEntriesRequest<K>> listener) {
        this.disposer.add(this.deleteEntriesRequestSubject
                .map(this.messageSerDe::deserializeDeleteEntriesRequest)
                .subscribe(listener)
        );
        return this;
    }

    public StorageEndpoint<K, V> onRemoteEndpointJoined(Consumer<UUID> listener) {
        this.disposer.add(this.grid.events().joinedRemoteEndpoints().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onRemoteEndpointDetached(Consumer<UUID> listener) {
        this.disposer.add(this.grid.events().detachedRemoteEndpoints().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onLeaderIdChanged(Consumer<Optional<UUID>> listener) {
        this.disposer.add(this.grid.events().changedLeaderId().subscribe(listener));
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

    public StorageEndpoint<K, V> onRemoveEntriesNotification(Consumer<RemoveEntriesNotification<K, V>> listener) {
        this.updateEntriesNotificationSubject
                .map(this.messageSerDe::deserializeRemoveEntriesNotification)
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
        this.dispatchNotification(message);
    }

    public void requestClearEntries() {
        this.requestClearEntries(null);
    }

    public void requestClearEntries(Set<UUID> destinationEndpointIds) {
        var request = new ClearEntriesRequest(UUID.randomUUID(), this.getLocalEndpointId());
        var message = this.messageSerDe.serializeClearEntriesRequest(request);
        this.request(message, destinationEndpointIds);
    }

    public void sendClearEntriesResponse(ClearEntriesResponse response) {
        var message = this.messageSerDe.serializeClearEntriesResponse(response);
        this.dispatchResponse(message);
    }


    public int requestGetSize() {
        return this.requestGetSize(null);
    }

    public int requestGetSize(Set<UUID> destinationEndpointIds) {
        var request = new GetSizeRequest(UUID.randomUUID(), null);
        var message = this.messageSerDe.serializeGetSizeRequest(request);
        var sum = this.request(message, destinationEndpointIds).stream()
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
        this.dispatchResponse(message);
    }

    public StorageEndpoint<K, V> onGetKeysRequest(Consumer<GetKeysRequest> listener) {
        this.getKeysRequestSubject
                .map(this.messageSerDe::deserializeGetKeysRequest)
                .subscribe(listener);
        return this;
    }

    public Set<K> requestGetKeys() {
        return this.requestGetKeys(null);
    }

    public Set<K> requestGetKeys(Set<UUID> destinationEndpointIds) {
        var request = new GetKeysRequest(UUID.randomUUID(), null);
        var message = this.messageSerDe.serializeGetKeysRequest(request);
        return this.request(message, destinationEndpointIds).stream()
                .map(this.messageSerDe::deserializeGetKeysResponse)
                .map(GetKeysResponse::keys)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public void sendGetKeysResponse(GetKeysResponse<K> response) {
        var message = this.messageSerDe.serializeGetKeysResponse(response);
        this.dispatchResponse(message);
    }

    public Map<K, V> requestGetEntries(Set<K> keys) {
        return this.requestGetEntries(keys, null);
    }

    public Map<K, V> requestGetEntries(Set<K> keys, Set<UUID> destinationEndpointIds) {
        var request = GetEntriesRequest.<K>builder()
                .setKeys(keys)
                .build();
        var message = this.messageSerDe.serializeGetEntriesRequest(request);
        var depot = depotProvider.get();
        var responses = this.request(message, destinationEndpointIds);
        responses.stream()
                .map(this.messageSerDe::deserializeGetEntriesResponse)
                .map(GetEntriesResponse::foundEntries)
                .forEach(depot::accept);
        var result = depot.get();
        logger.debug("{} collected responses: {}", this.getLocalEndpointId(),
                responses.stream().map(Object::toString).collect(Collectors.toList()));
        return result;
    }

    public void sendGetEntriesResponse(GetEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeGetEntriesResponse(response);
        logger.debug("{} sending {} response to {}", this.grid.endpoints().getLocalEndpointId().toString().substring(0, 8), message.type, message.destinationId);
        this.dispatchResponse(message);
    }

    public Set<K> requestDeleteEntries(Set<K> keys) {
        return this.requestDeleteEntries(keys, null);
    }

    public Set<K> requestDeleteEntries(Set<K> keys, Set<UUID> destinationEndpointIds) {
        var request = DeleteEntriesRequest.<K>builder()
                .setKeys(keys)
                .build();
        var message = this.messageSerDe.serializeDeleteEntriesRequest(request);
        return this.request(message, destinationEndpointIds).stream()
                .map(this.messageSerDe::deserializeDeleteEntriesResponse)
                .map(DeleteEntriesResponse::deletedKeys)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    public void sendDeleteEntriesNotification(DeleteEntriesNotification<K> notification) {
        var message = this.messageSerDe.serializeDeleteEntriesNotification(notification);
        this.dispatchNotification(message);
    }

    public void sendDeleteEntriesResponse(DeleteEntriesResponse<K> response) {
        var message = this.messageSerDe.serializeDeleteEntriesResponse(response);
        this.dispatchResponse(message);
    }

    public void sendUpdateEntriesNotification(UpdateEntriesNotification<K, V> notification) {
        var message = this.messageSerDe.serializeUpdateEntriesNotification(notification);
        this.dispatchNotification(message);
    }

    public void sendRemoveEntriesNotification(RemoveEntriesNotification<K, V> notification) {
        var message = this.messageSerDe.serializeRemoveEntriesNotification(notification);
        this.dispatchNotification(message);
    }

    public Map<K, V> requestUpdateEntries(Map<K, V> entries) {
        return this.requestUpdateEntries(entries, null);
    }

    public Map<K, V> requestUpdateEntries(Map<K, V> entries, Set<UUID> destinationEndpointIds) {
        var request = UpdateEntriesRequest.<K, V>builder()
                .setEntries(entries)
                .build();
        var message = this.messageSerDe.serializeUpdateEntriesRequest(request);
        var depot = depotProvider.get();
        this.request(message, destinationEndpointIds).stream()
                .map(this.messageSerDe::deserializeUpdateEntriesResponse)
                .map(UpdateEntriesResponse::entries)
                .forEach(depot::accept);
        return depot.get();
    }

    public Map<K, V> requestInsertEntries(Map<K, V> entries) {
        return this.requestInsertEntries(entries, null);
    }

    public Map<K, V> requestInsertEntries(Map<K, V> entries, Set<UUID> destinationEndpointIds) {
        var request = new InsertEntriesRequest(UUID.randomUUID(), entries, this.grid.endpoints().getLocalEndpointId());
        var message = this.messageSerDe.serializeInsertEntriesRequest(request);
        var depot = depotProvider.get();
        this.request(message, destinationEndpointIds).stream()
                .map(this.messageSerDe::deserializeInsertEntriesResponse)
                .map(InsertEntriesResponse::existingEntries)
                .forEach(depot::accept);
        return depot.get();
    }

    public void sendInsertEntriesNotification(InsertEntriesNotification<K, V> notification) {
        var message = this.messageSerDe.serializeInsertEntriesNotification(notification);
        this.dispatchNotification(message);
    }

    public void sendInsertEntriesResponse(InsertEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeInsertEntriesResponse(response);
        this.dispatchResponse(message);
    }

    public void sendUpdateEntriesResponse(UpdateEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeUpdateEntriesResponse(response);
        this.dispatchResponse(message);
    }

    private void processResponse(Message message) {
        if (message.requestId == null) {
            logger.warn("RequestId is null in response {}", message);
            return;
        }

        var chunkedResponse = message.sequence != null && message.lastMessage != null;
        var onlyOneChunkExists = ZERO.equals(message.sequence) && Boolean.TRUE.equals(message.lastMessage);
        if (chunkedResponse && !onlyOneChunkExists) {
            var pendingResponse = this.pendingResponses.get(message.requestId);
            if (pendingResponse == null) {
                pendingResponse = new PendingResponse();
                this.pendingResponses.put(message.requestId, pendingResponse);
            }
            pendingResponse.accept(message);
            if (!pendingResponse.isReady()) {
                return;
            }
            message = pendingResponse.getResult();
            this.pendingResponses.remove(message.requestId);
        }
        logger.trace("{} Receiving message for request id {} for type: {}", this.grid.endpoints().getLocalEndpointId(), message.requestId, message.type);
        var pendingRequest = this.pendingRequests.get(message.requestId);
        if (pendingRequest == null) {
            logger.warn("{} No pending request found for message {}", this.grid.endpoints().getLocalEndpointId(), message);
            return;
        }
        pendingRequest.accept(message);
    }

    private List<Message> request(Message message, Set<UUID> destinationEndpointIds) throws FailedOperationException {
        return this.request(message, destinationEndpointIds, 0);
    }

    private List<Message> request(Message message, Set<UUID> destinationEndpointIds, int retried) throws FailedOperationException {
        if (1 < retried) {
            throw new FailedOperationException("Cannot resolve request " + message);
        }
        var requestId = message.requestId;
        Set<UUID> remoteEndpointIds;
        if (destinationEndpointIds != null && 0 < destinationEndpointIds.size()) {
            remoteEndpointIds = destinationEndpointIds;
        } else if (message.destinationId != null) {
            remoteEndpointIds = Set.of(message.destinationId);
        } else {
            remoteEndpointIds = this.defaultResolvingEndpointIds(MessageType.valueOfOrNull(message.type));
        }
        if (remoteEndpointIds != null && remoteEndpointIds.size() == 1) {
            message.destinationId = remoteEndpointIds.stream().findFirst().get();
        }
        logger.debug("Creating request ({}) ({} - {}) remote endpoints: {}", requestId, this.grid.endpoints().getLocalEndpointId(), this.grid.getContext(), remoteEndpointIds);
        if (remoteEndpointIds.size() < 1) {
            return Collections.emptyList();
        }
        var pendingRequest = PendingRequest.builder()
                .withRequestId(requestId)
                .withPendingEndpoints(remoteEndpointIds)
                .withTimeoutInMs(this.grid.getRequestTimeoutInMs() * (retried + 1))
                .build();
        pendingRequest.onCompleted(() -> {
            logger.trace("Request {} (type: {}) is completed", requestId, message.type);
        });
        this.pendingRequests.put(requestId, pendingRequest);

        logger.debug("Sending request (type: {}, id: {}), PendingRequest: {}", message.type, requestId, pendingRequest);

        this.dispatchRequest(message);

        try {
//            logger.info("pendingRequest.get {}", Thread.currentThread().getId());
            var result =  pendingRequest.get();
            this.pendingRequests.remove(requestId);
            logger.debug("Request {} (type: {}) is removed", requestId, message.type);
            return result;
        } catch (ExecutionException e) {
            logger.warn("Error occurred while processing request {} on endpoint {}, message type {}, protocol: {}",
                    requestId,
                    this.getLocalEndpointId(),
                    message.type,
                    message.protocol,
                    e
            );
            this.pendingRequests.remove(requestId);
            message.requestId = UUID.randomUUID();
            return this.request(message, destinationEndpointIds, retried + 1);
        } catch (InterruptedException e) {
            logger.warn("Error occurred while processing request {} on endpoint {}, message type {}, protocol: {}",
                    requestId,
                    this.getLocalEndpointId(),
                    message.type,
                    message.protocol,
                    e
            );
            this.pendingRequests.remove(requestId);
            message.requestId = UUID.randomUUID();
            return this.request(message, destinationEndpointIds, retried + 1);
        } catch (TimeoutException e) {
            logger.warn("Timeout occurred while processing request {} on endpoint {}, message type {}, protocol: {}",
                    requestId,
                    this.getLocalEndpointId(),
                    message.type,
                    message.protocol,
                    e
            );
            this.pendingRequests.remove(requestId);
            message.requestId = UUID.randomUUID();
            return this.request(message, destinationEndpointIds, retried + 1);
        }
    }

    private void dispatchResponse(Message message) {
        message.storageId = this.getStorageId();
        message.protocol = this.protocol;
        var it = this.responseMessageChunker.apply(message);
        if (it == null) {
            logger.warn("No iterator returned to chunk response. the response itself will be sent unchunked");
            this.sendResponse(message);
            return;
        }
        while (it.hasNext()) {
            var chunk = it.next();
            this.sendResponse(chunk);
        }
    }

    private void dispatchNotification(Message message) {
        message.storageId = this.getStorageId();
        message.protocol = this.protocol;
        if (message.sourceId != null && message.destinationId != null && UuidTools.equals(message.sourceId, message.destinationId)) {
            this.receive(message);
            return;
        }
        this.sendNotification(message);
    }

    private void dispatchRequest(Message message) {
        message.storageId = this.getStorageId();
        message.protocol = this.protocol;
        this.sendRequest(message);
    }

    protected abstract Set<UUID> defaultResolvingEndpointIds(MessageType messageType);
    protected abstract void sendNotification(Message message);
    protected abstract void sendRequest(Message message);
    protected abstract void sendResponse(Message message);

    @Override
    public void close() {
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
    }
}
