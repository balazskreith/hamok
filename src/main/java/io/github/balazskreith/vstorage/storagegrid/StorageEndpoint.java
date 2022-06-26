package io.github.balazskreith.vstorage.storagegrid;

import io.github.balazskreith.vstorage.common.Depot;
import io.github.balazskreith.vstorage.common.Disposer;
import io.github.balazskreith.vstorage.common.JsonUtils;
import io.github.balazskreith.vstorage.storagegrid.messages.*;
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
    StorageOpSerDe<K, V> messageSerDe;
    String storageId;
    String protocol;

    final Disposer disposer;
    // created by the constructor
    private final Map<UUID, PendingRequest> pendingRequests = new ConcurrentHashMap<>();
    private final Subject<Message> getEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> deleteEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> insertEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> updateEntriesRequestSubject = PublishSubject.create();
    private final Subject<Message> insertEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> updateEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> deleteEntriesNotificationSubject = PublishSubject.create();
    private final Subject<Message> evictEntriesNotificationSubject = PublishSubject.create();

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
                .addSubject(this.evictEntriesNotificationSubject)
                .addSubject(this.insertEntriesRequestSubject)
                .onCompleted(() -> {
                    logger.info("Disposed. StorageId: {}, protocol: {}", this.storageId, this.protocol);
                })
                .build();
    }

    public String getStorageId() {
        return this.storageId;
    }

    public Set<UUID> getRemoteEndpointIds() {
        return this.grid.getRemoteEndpointIds();
    }

    void receive(Message message) {
        if (message.protocol != null && !message.protocol.equals(this.protocol)) {
            logger.debug("Ignore received message {}, message protocol: {}, endpoint protocol: {}", message.type, message.protocol, this.protocol);
            // this is not for this endpoint
            return;
        }
        var type = MessageType.valueOf(message.type);
        switch (type) {
            case GET_ENTRIES_REQUEST -> this.getEntriesRequestSubject.onNext(message);
            case UPDATE_ENTRIES_REQUEST -> this.updateEntriesRequestSubject.onNext(message);
            case UPDATE_ENTRIES_NOTIFICATION -> this.updateEntriesNotificationSubject.onNext(message);
            case INSERT_ENTRIES_REQUEST -> this.insertEntriesRequestSubject.onNext(message);
            case DELETE_ENTRIES_REQUEST -> this.deleteEntriesRequestSubject.onNext(message);
            case DELETE_ENTRIES_NOTIFICATION -> this.deleteEntriesNotificationSubject.onNext(message);
            case EVICT_ENTRIES_NOTIFICATION -> this.evictEntriesNotificationSubject.onNext(message);
            case DELETE_ENTRIES_RESPONSE,
                    GET_ENTRIES_RESPONSE,
                    INSERT_ENTRIES_RESPONSE,
                    UPDATE_ENTRIES_RESPONSE -> this.processResponse(message);
            default -> {
                logger.warn("Message type is not recognized {} ", JsonUtils.objectToString(message));
            }
        }
    }

    public StorageEndpoint<K, V> onDeleteEntriesRequest(Consumer<DeleteEntriesRequest<K>> listener) {
        this.disposer.add(this.deleteEntriesRequestSubject
                .map(this.messageSerDe::deserializeDeleteEntriesRequest)
                .subscribe(listener)
        );
        return this;
    }

    public StorageEndpoint<K, V> onEvictedEntriesNotification(Consumer<EvictEntriesNotification<K>> listener) {
        this.evictEntriesNotificationSubject
                .map(this.messageSerDe::deserializeEvictEntriesNotification)
                .subscribe(listener);
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

    public StorageEndpoint<K, V> onLocalEndpointReset(Consumer<Void> listener) {
        this.disposer.add(this.grid.inactivatedLocalEndpoint().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onLeaderIdChanged(Consumer<Optional<UUID>> listener) {
        this.disposer.add(this.grid.changedLeaderId().subscribe(listener));
        return this;
    }

    public StorageEndpoint<K, V> onDeleteEntriesNotification(Consumer<EvictEntriesNotification<K>> listener) {
        this.deleteEntriesNotificationSubject
                .map(this.messageSerDe::deserializeEvictEntriesNotification)
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

    public Map<K, V> requestGetEntries(Set<K> keys) {
        var request = GetEntriesRequest.<K>builder()
                .setKeys(keys)
                .build();
        var message = this.messageSerDe.serializeGetEntriesRequest(request);
        var depot = depotProvider.get();
        this.request(message).stream()
                .map(this.messageSerDe::deserializeGetEntriesResponse)
                .map(GetEntriesResponse::foundEntries)
                .forEach(depot::accept);
        return depot.get();
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

    public Set<K> submitRequestDeleteEntries(Set<K> keys) {
        var request = DeleteEntriesRequest.<K>builder()
                .setKeys(keys)
                .build();
        var message = this.messageSerDe.serializeDeleteEntriesRequest(request);
        return this.submitRequest(message).stream()
                .map(this.messageSerDe::deserializeDeleteEntriesResponse)
                .map(DeleteEntriesResponse::deletedKeys)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
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

    public Map<K, V> submitRequestUpdateEntries(Map<K, V> entries) {
        var request = UpdateEntriesRequest.<K, V>builder()
                .setEntries(entries)
                .build();
        var message = this.messageSerDe.serializeUpdateEntriesRequest(request);
        var depot = depotProvider.get();
        this.submitRequest(message).stream()
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

    public Map<K, V> submitRequestInsertEntries(Map<K, V> entries) {
        var request = new InsertEntriesRequest(UUID.randomUUID(), entries, this.grid.getLocalEndpointId());
        var message = this.messageSerDe.serializeInsertEntriesRequest(request);
        var depot = depotProvider.get();
        this.submitRequest(message).stream()
                .map(this.messageSerDe::deserializeInsertEntriesResponse)
                .map(InsertEntriesResponse::existingEntries)
                .forEach(depot::accept);
        return depot.get();
    }

    public void sendUpdateInsertEntriesNotification(InsertEntriesNotification<K, V> notification) {
        var message = this.messageSerDe.serializeInsertEntriesNotification(notification);
        this.send(message);
    }

    public void sendUpdateEntriesResponse(UpdateEntriesResponse<K, V> response) {
        var message = this.messageSerDe.serializeUpdateEntriesResponse(response);
        this.send(message);
    }

    public void sendEvictedEntriesNotification(EvictEntriesNotification<K> notification) {
        var message = this.messageSerDe.serializeEvictEntriesNotification(notification);
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
            logger.warn("{} No pending request found for message {}", this.grid.getLocalEndpointId().toString().substring(0, 8) ,JsonUtils.objectToString(message));
            return;
        }
        logger.debug("{} Receiving response for request id {} for type: {}",this.grid.getLocalEndpointId().toString().substring(0, 8), message.requestId, message.type);
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
            remoteEndpointIds = this.grid.getRemoteEndpointIds();
        }
//        logger.info("Creating request at {} remote endpoints: {}", this.grid.getLocalEndpointId(), JsonUtils.objectToString(remoteEndpointIds));
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
        this.send(message);

        try {
            var result =  pendingRequest.get();
            this.pendingRequests.remove(requestId);
            return result;
        } catch (ExecutionException e) {
            logger.warn("Error occurred while processing request {} ", requestId , e);
            this.pendingRequests.remove(requestId);
            return this.request(message, retried + 1);
        } catch (InterruptedException e) {
            logger.warn("Error occurred while processing request {} ", requestId, e);
            this.pendingRequests.remove(requestId);
            return this.request(message, retried + 1);
        } catch (TimeoutException e) {
            logger.warn("Timeout occurred while processing request {} ", requestId, e);
            this.pendingRequests.remove(requestId);
            return this.request(message, retried + 1);
        }
    }

    private List<Message> submitRequest(Message message) {
        return this.submitRequest(message, 0);
    }

    private List<Message> submitRequest(Message message, int retried) {
        if (3 < retried) {
            logger.error("Cannot resolve submitted request", JsonUtils.objectToString(message));
            return Collections.emptyList();
        }
        var requestId = message.requestId;
        // it only resolved by one and only one response.
        var pendingRequest = PendingRequest.builder()
                .withRequestId(requestId)
                .withNeededResponse(1)
                .withTimeoutInMs(this.grid.getRequestTimeoutInMs() * (retried + 1))
                .build();

        this.pendingRequests.put(requestId, pendingRequest);

        // there is only one response we are waiting for, and only the leader will respond
        this.submit(message);

        try {
            return pendingRequest.get();
        } catch (ExecutionException e) {
            logger.warn("Failed to resolve requestId {}. Retried: {}", requestId, retried, e);
            return this.submitRequest(message, retried + 1);
        } catch (InterruptedException e) {
            logger.warn("Failed to resolve requestId {}. Retried: {}", requestId, retried, e);
            return this.submitRequest(message, retried + 1);
        } catch (TimeoutException e) {
            e.printStackTrace();
            logger.warn("Timed out request to resolve requestId {}. Retried: {}. Next iteration the timeout will be increased to {}", requestId, retried, (retried + 2) * this.grid.getRequestTimeoutInMs(), e);
            return this.submitRequest(message, retried + 1);
        }
    }

    private void send(Message message) {
        message.storageId = this.storageId;
        message.protocol = this.protocol;
        this.grid.send(message);
    }

    private void submit(Message message) {
        message.storageId = this.storageId;
        message.protocol = this.protocol;
        this.grid.submit(message);
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
