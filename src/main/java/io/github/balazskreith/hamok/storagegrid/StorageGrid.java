package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.raccoons.Raccoon;
import io.github.balazskreith.hamok.raccoons.events.HelloNotification;
import io.github.balazskreith.hamok.storagegrid.messages.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StorageGrid implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(StorageGrid.class);

    public static StorageGridBuilder builder() {
        return new StorageGridBuilder();
    }

    private final Map<UUID, CompletableFuture<Message>> pendingRequests = new ConcurrentHashMap<>();
    private final Map<String, DistributedStorageOperations> storageOperations = new ConcurrentHashMap<>();
    private final GridOpSerDe gridOpSerDe = new GridOpSerDe();
    private final Subject<byte[]> sender = PublishSubject.<byte[]>create().toSerialized();
    private final Subject<byte[]> receiver = PublishSubject.<byte[]>create().toSerialized();

    private final Codec<Message, byte[]> messageCodec;
    private final Disposer disposer;
    private final StorageGridConfig config;
    private final StorageGridMetrics metrics = new StorageGridMetrics();

    private final Raccoon raccoon;
    private final String context;
    private final StorageGridTransport transport;

    StorageGrid(
            StorageGridConfig config,
            Raccoon raccoon,
            Codec<Message, byte[]> messageCodec,
            String context
    ) {
        this.config = config;
        this.raccoon = raccoon;
        this.messageCodec = messageCodec;
        this.context = context;
        this.transport = new StorageGridTransport() {
            @Override
            public Observer<byte[]> getReceiver() {
                return receiver;
            }

            @Override
            public Observable<byte[]> getSender() {
                return sender;
            }
        };

        this.disposer = Disposer.builder()
                .addDisposable(this.raccoon)
                .addSubject(this.sender)
                .addSubject(this.receiver)
                .addDisposable(this.receiver.subscribe(bytes -> {
                    Message message;
                    try {
                        message = this.messageCodec.decode(bytes);
                    } catch (Exception ex) {
                        logger.warn("{} Cannot decode message", this.context, ex);
                        return;
                    }
                    logger.info("{} received message (type: {}) from {}", this.getLocalEndpointId().toString().substring(0, 8), message.type, message.sourceId.toString().substring(0, 8));
                    if (message.destinationId == null || UuidTools.equals(message.destinationId, this.getLocalEndpointId())) {
                        this.dispatch(message);
                    } else {
                        logger.warn("Message destination {} is not for this local endpoint {} {}", message.destinationId, this.getLocalEndpointId());
                    }

                }))
                .addDisposable(this.raccoon.outboundEvents().appendEntriesRequest().subscribe(raftAppendEntriesRequest -> {
                    var message = this.gridOpSerDe.serializeRaftAppendRequest(raftAppendEntriesRequest);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.outboundEvents().appendEntriesResponse().subscribe(raftAppendEntriesResponse -> {
                    var message = this.gridOpSerDe.serializeRaftAppendResponse(raftAppendEntriesResponse);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.outboundEvents().voteRequests().subscribe(raftVoteRequest -> {
                    var message = this.gridOpSerDe.serializeRaftVoteRequest(raftVoteRequest);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.outboundEvents().voteResponse().subscribe(raftVoteResponse -> {
                    var message = this.gridOpSerDe.serializeRaftVoteResponse(raftVoteResponse);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.joinedRemotePeerId().subscribe(remoteEndpointId -> {

                }))
                .addDisposable(this.raccoon.detachedRemotePeerId().subscribe(remoteEndpointId -> {

                }))
                .addDisposable(this.raccoon.outboundEvents().endpointStateNotifications().subscribe(notification -> {
                    var message = this.gridOpSerDe.serializeEndpointStatesNotification(notification);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.outboundEvents().helloNotifications().subscribe(notification -> {
                    var leaderId = this.getLeaderId();
                    if (leaderId != null) {
                        notification = new HelloNotification(notification.sourcePeerId(), leaderId);
                    }
                    var message = this.gridOpSerDe.serializeHelloNotification(notification);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.changedLeaderId().subscribe(newLeaderId -> {

                }))
                .addDisposable(this.raccoon.committedEntries().subscribe(logEntry -> {
                    // a commit should go to the upper layer.
                    // if it is not the leader then a request must be converted to notification,
                    // and it must be because when a new follower later join
                    // it should not be replied to a previous request
                    // therefore the request is responded by one and only one member of the grid, the leader
                    var message = this.messageCodec.decode(logEntry.entry());
                    if (UuidTools.equals(this.getLocalEndpointId(), this.getLeaderId())) {
                        this.dispatch(message);
                        return;
                    }
                    var type = MessageType.valueOfOrNull(message.type);
                    if (type == MessageType.DELETE_ENTRIES_REQUEST) {
                        message.type = MessageType.DELETE_ENTRIES_NOTIFICATION.name();
                    } else if (type == MessageType.UPDATE_ENTRIES_REQUEST) {
                        message.type = MessageType.UPDATE_ENTRIES_NOTIFICATION.name();
                    } else if (type == MessageType.INSERT_ENTRIES_REQUEST) {
                        message.type = MessageType.INSERT_ENTRIES_NOTIFICATION.name();
                    }
                    this.dispatch(message);
                }))
                .addDisposable(this.raccoon.commitIndexSyncRequests().subscribe(signal -> {
                    // this cannot be requested by the leader only the follower, so
                    // we are "sure" that our local endpoint is not the leader endpoint.
                    UUID requestId = UUID.randomUUID();
                    var request = new StorageSyncRequest(requestId, this.getLocalEndpointId());
                    var promise = new CompletableFuture<Message>();
                    this.pendingRequests.put(requestId, promise);
                    var message = this.gridOpSerDe.serializeStorageSyncRequest(request);
                    this.send(message);

                    promise.thenAccept(respondedMessage -> {
                        var response = this.gridOpSerDe.deserializeStorageSyncResponse(respondedMessage);
                        for (var it = response.storageUpdateNotifications().entrySet().iterator(); it.hasNext(); ) {
                            var entry = it.next();
                            var storageId = entry.getKey();
                            var updateNotification = entry.getValue();
                            var storageOperations = this.storageOperations.get(storageId);
                            if (storageOperations == null) {
                                logger.warn("{} Not found storage for storageSync response {}", this.context, storageId);
                                continue;
                            }
                            if (storageOperations.getStorageClassSimpleName().equals(ReplicatedStorage.class.getSimpleName())) {
                                Message updateNotificationMessage = null;
                                try {
                                    updateNotificationMessage = this.messageCodec.decode(updateNotification);
                                } catch (Throwable e) {
                                    logger.warn("{} Update notification deserialization failed", this.context, e);
                                    continue;
                                }
                                // Let's Rock
                                storageOperations.storageLocalClear();
                                this.dispatch(updateNotificationMessage);
                                logger.info("{} Storage {} is synchronized with the leader.", this.context, storageOperations.getStorageId());
                            }
                        }
                        this.raccoon.setCommitIndex(response.commitIndex());
                    });
                }))
                .onCompleted(() -> {
                    logger.info("{} ({}) is disposed", this.getLocalEndpointId(), this.getContext());
                })
                .build();
        logger.info("{} ({}) is created", this.getLocalEndpointId(), this.getContext());
        this.raccoon.start();
    }

    private void dispatch(Message message) {
        var type = MessageType.valueOfOrNull(message.type);
        if (type == null) {
            logger.warn("{} ({}) received an unrecognized message {}", this.getLocalEndpointId(), this.context, JsonUtils.objectToString(message));
            return;
        }
        logger.debug("{} ({}) received message {}", this.getLocalEndpointId(), this.context, JsonUtils.objectToString(message));
        switch (type) {
            case HELLO_NOTIFICATION -> {
                var notification = this.gridOpSerDe.deserializeHelloNotification(message);
                this.raccoon.inboundEvents().helloNotifications().onNext(notification);
            }
            case ENDPOINT_STATES_NOTIFICATION -> {
                var notification = this.gridOpSerDe.deserializeEndpointStatesNotification(message);
                this.raccoon.inboundEvents().endpointStateNotifications().onNext(notification);
            }
            case RAFT_APPEND_ENTRIES_REQUEST -> {
                var appendEntriesRequest = this.gridOpSerDe.deserializeRaftAppendRequest(message);
                this.raccoon.inboundEvents().appendEntriesRequests().onNext(appendEntriesRequest);
            }
            case RAFT_APPEND_ENTRIES_RESPONSE -> {
                var appendEntriesResponse = this.gridOpSerDe.deserializeRaftAppendResponse(message);
                this.raccoon.inboundEvents().appendEntriesResponses().onNext(appendEntriesResponse);
            }
            case RAFT_VOTE_REQUEST -> {
                var voteRequest = this.gridOpSerDe.deserializeRaftVoteRequest(message);
                this.raccoon.inboundEvents().voteRequests().onNext(voteRequest);
            }
            case RAFT_VOTE_RESPONSE -> {
                var voteResponse = this.gridOpSerDe.deserializeRaftVoteResponse(message);
                this.raccoon.inboundEvents().voteResponse().onNext(voteResponse);
            }
            case STORAGE_SYNC_REQUEST -> {
                var request = this.gridOpSerDe.deserializeStorageSyncRequest(message);
                if (this.getLeaderId() != this.getLocalEndpointId()) {
                    // not successful
                    var response = request.createResponse(
                            -1,
                            Collections.emptyMap(),
                            false,
                            this.getLeaderId()
                    );
                    var responseMessage = this.gridOpSerDe.serializeStorageSyncResponse(response);
                    this.send(responseMessage);
                    return;
                }
                Map<String, byte[]> storageUpdateNotifications = new HashMap<>();
                int savedCommitIndex = this.raccoon.getCommitIndex();
                for (var it = this.storageOperations.values().iterator(); it.hasNext(); ) {
                    var storageOperations = it.next();
                    if (storageOperations == null) continue; // not ready
                    if (storageOperations.getStorageClassSimpleName().equals(ReplicatedStorage.class.getSimpleName())) {
                        var updateNotificationMessage = storageOperations.getAllKeysUpdateNotification(request.sourceEndpointId());
                        byte[] storageEndpointNotification;
                        try {
                            storageEndpointNotification = this.messageCodec.encode(updateNotificationMessage);
                        } catch (Throwable e) {
                            logger.warn("{} ({}) Error while serializing message", this.getLocalEndpointId(), this.context, e);
                            continue;
                        }
                        storageUpdateNotifications.put(storageOperations.getStorageId(), storageEndpointNotification);
                    }
                }

                var response = request.createResponse(
                        savedCommitIndex,
                        storageUpdateNotifications,
                        true,
                        this.getLeaderId()
                );
                var respondingMessage = this.gridOpSerDe.serializeStorageSyncResponse(response);
                this.send(respondingMessage);
            }
            case STORAGE_SYNC_RESPONSE -> {
                var response = this.gridOpSerDe.deserializeStorageSyncResponse(message);
                if (!response.success()) {
                    logger.info("{} Sending Storage sync again to another leader. prev assumed leader: {}, the leader we send now: {}", this.context, message.sourceId, response.leaderId());
                    var newRequest = new StorageSyncRequest(response.requestId(), response.leaderId());
                    var newMessage = this.gridOpSerDe.serializeStorageSyncRequest(newRequest);
                    this.send(newMessage);
                    return;
                }
                var pendingRequest = this.pendingRequests.get(response.requestId());
                if (pendingRequest == null) {
                    logger.warn("{} There is no Storage Sync pending request for requestId {}", this.context, response.requestId());
                    return;
                }
                pendingRequest.complete(message);
            }
            case SUBMIT_REQUEST -> {
                var request = this.gridOpSerDe.deserializeSubmitRequest(message);
                var response = this.gridOpSerDe.serializeSubmitResponse(request.createResponse(
                        message.sourceId,
                        this.raccoon.submit(request.entry()) != null,
                        this.raccoon.getLeaderId()
                ));
                this.send(response);
            }
            case SUBMIT_RESPONSE -> {
                var requestId = message.requestId;
                if (requestId == null) {
                    logger.warn("{} No requestId attached for response {}", this.context, JsonUtils.objectToString(message));
                    return;
                }
                var pendingRequest = this.pendingRequests.get(requestId);
                if (pendingRequest == null) {
                    logger.warn("{} No pending request is registered for response {}", this.context, JsonUtils.objectToString(message));
                    return;
                }
                pendingRequest.complete(message);
            }
            default -> {
                var storageId = message.storageId;
                if (storageId == null) {
                    logger.warn("{} No StorageId is defined for message {}", this.context, JsonUtils.objectToString(message));
                    return;
                }
                var storageOperations = this.storageOperations.get(storageId);
                if (storageOperations == null) {
                    logger.warn("{} No message acceptor for storage {}. Message: {}", this.context, storageId, JsonUtils.objectToString(message));
                    return;
                }
                storageOperations.accept(message);
            }
        }
    }

    public <K, V> SeparatedStorageBuilder<K, V> separatedStorage() {
        var storageGrid = this;
        var operationsBuilder = DistributedStorageOperations.<K, V>builder();
        return new SeparatedStorageBuilder<K, V>()
                .setMapDepotProvider(MapUtils::makeMapAssignerDepot)
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    operationsBuilder.withEndpoint(storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    var operations = operationsBuilder.withStorage(storage).build();
                    this.storageOperations.put(storage.getId(), operations);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.storageOperations::remove);
                })
                ;
    }

    public <K, V> FederatedStorageBuilder<K, V> federatedStorage() {
        var storageGrid = this;
        var operationsBuilder = DistributedStorageOperations.<K, V>builder();
        return new FederatedStorageBuilder<K, V>()
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    operationsBuilder.withEndpoint(storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    var operations = operationsBuilder.withStorage(storage).build();
                    this.storageOperations.put(storage.getId(), operations);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.storageOperations::remove);
                })
                ;
    }

    public <K, V> ReplicatedStorageBuilder<K, V> replicatedStorage() {
        var storageGrid = this;
        var operationsBuilder = DistributedStorageOperations.<K, V>builder();
        return new ReplicatedStorageBuilder<K, V>()
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    operationsBuilder.withEndpoint(storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    operationsBuilder.withStorage(storage);
                    this.storageOperations.put(storage.getId(), operationsBuilder.build());
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.storageOperations::remove);
                })
                ;
    }

    public void addRemoteEndpointId(UUID endpointId) {
        this.raccoon.addRemotePeerId(endpointId);
    }

    public void removeRemoteEndpointId(UUID endpointId) {
        this.raccoon.removeRemotePeerId(endpointId);
    }

    public StorageGridTransport transport() {
        return StorageGridTransport.create(this.receiver, this.sender);
    }

    Set<UUID> getRemoteEndpointIds() {
        return this.raccoon.getRemoteEndpointIds();
    }

    int getRequestTimeoutInMs() {
        return this.config.requestTimeoutInMs();
    }

    void send(Message message) {
        logger.info("{} sending message (type: {}) to {}", this.getLocalEndpointId().toString().substring(0, 8), message.type, message.destinationId);
        byte[] bytes;
        try {
            message.sourceId = this.raccoon.getId();
            bytes = this.messageCodec.encode(message);
        } catch (Throwable e) {
            logger.warn("{} Cannot encode message {}", this.context, JsonUtils.objectToString(message), e);
            return;
        }
        this.sender.onNext(bytes);
    }

    void submit(Message message) {
        var leaderId = this.raccoon.getLeaderId();
        if (leaderId == null) {
            var waitForLeader = new CompletableFuture<Optional<UUID>>();
            this.raccoon.changedLeaderId().firstElement().subscribe(waitForLeader::complete);
            try {
                var maybeNewLeaderId = waitForLeader.get(3000, TimeUnit.MILLISECONDS);
                leaderId = maybeNewLeaderId.orElse(null);
            } catch (Exception e) {
                logger.warn("{} Exception occurred while waiting for leaders", this.context, e);
            }

            if (leaderId == null) {
                this.submit(message);
                return;
            }
        }
        byte[] entry;
        try {
            entry = this.messageCodec.encode(message);
        } catch (Throwable e) {
            logger.warn("{} Error occurred while encoding entry", this.context, e);
            this.submit(message);
            return;
        }
        if (leaderId == this.raccoon.getId()) {
            // we can submit here.
            if (this.raccoon.submit(entry) == null) {
                logger.warn("{} submitting message to the leader returned with null.", this.context);
                this.submit(message);
                return;
            }
            return;
        }
        // we need to send it to the leader in an embedded message
        var promise = new CompletableFuture<Message>();
        var record = new SubmitRequest(UUID.randomUUID(), entry);
        var request = this.gridOpSerDe.serializeSubmitRequest(record);
        this.pendingRequests.put(request.requestId, promise);

        this.send(request);

        try {
            promise.get(this.getRequestTimeoutInMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            this.pendingRequests.remove(request.requestId);
            logger.warn("{} Error occurred while waiting for submit response", this.context, e);
        } finally {
            this.pendingRequests.remove(request.requestId);
        }
    }

    Observable<UUID> joinedRemoteEndpoints() {
        return this.raccoon.joinedRemotePeerId();
    }

    Observable<UUID> detachedRemoteEndpoints() {
        return this.raccoon.detachedRemotePeerId();
    }

    Observable<Long> inactivatedLocalEndpoint() {
        return this.raccoon.inactivatedLocalPeer();
    }

    Observable<Optional<UUID>> changedLeaderId() {
        return this.raccoon.changedLeaderId();
    }

    String getContext() {
        return this.context;
    }

    UUID getLeaderId() {
        return this.raccoon.getLeaderId();
    }

    UUID getLocalEndpointId() {
        return this.raccoon.getId();
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
