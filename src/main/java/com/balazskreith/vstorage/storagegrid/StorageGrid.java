package com.balazskreith.vstorage.storagegrid;

import com.balazskreith.vstorage.common.Disposer;
import com.balazskreith.vstorage.common.JsonUtils;
import com.balazskreith.vstorage.common.MapUtils;
import com.balazskreith.vstorage.mappings.Codec;
import com.balazskreith.vstorage.raft.RxRaft;
import com.balazskreith.vstorage.raft.events.RaftTransport;
import com.balazskreith.vstorage.rxutils.RxCollector;
import com.balazskreith.vstorage.rxutils.RxEmitter;
import com.balazskreith.vstorage.storagegrid.discovery.Discovery;
import com.balazskreith.vstorage.storagegrid.messages.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StorageGrid implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(StorageGrid.class);

    private final Map<UUID, CompletableFuture<Message>> pendingRequests = new ConcurrentHashMap<>();
    private final Map<String, CorrespondedStorage> storages = new ConcurrentHashMap<>();
    private final GridOpSerDe gridOpSerDe = new GridOpSerDe();

    private final Codec<Message, byte[]> messageCodec;
    private final Disposer disposer;
    private final StorageGridConfig config;
    private final RxCollector<byte[]> sender;
    private final RxEmitter<byte[]> receiver;
    private final RxRaft raft;
    private final Discovery discovery;
    private final RaftTransport raftTransport;


    StorageGrid(
            StorageGridConfig config,
            RxEmitter<byte[]> receiver,
            RxCollector<byte[]> sender,
            RxRaft rxRaft,
            Discovery discovery,
            Codec<Message, byte[]> messageCodec
            ) {
        this.config = config;
        this.receiver = receiver;
        this.sender = sender;
        this.raft = rxRaft;
        this.discovery = discovery;
        this.messageCodec = messageCodec;
        this.raftTransport = this.raft.transport();

        this.disposer = Disposer.builder()
                .addDisposable(this.raft)
                .addDisposable(this.discovery)
                .addDisposable(this.sender::isTerminated, this.sender::onComplete)
                .addDisposable(this.receiver::isTerminated, this.receiver::onComplete)
                .addDisposable(this.receiver.subscribe(bytes -> {
                    Message message;
                    try {
                        message = this.messageCodec.decode(bytes);
                    } catch(Exception ex) {
                        logger.warn("Cannot decode message", ex);
                        return;
                    }
                    this.dispatch(message);
                }))
                .addDisposable(this.raftTransport.sender().appendEntriesRequest().subscribe(raftAppendEntriesRequest -> {
                    var message = this.gridOpSerDe.serializeRaftAppendRequest(raftAppendEntriesRequest);
                    this.send(message);
                }))
                .addDisposable(this.raftTransport.sender().appendEntriesResponse().subscribe(raftAppendEntriesResponse -> {
                    var message = this.gridOpSerDe.serializeRaftAppendResponse(raftAppendEntriesResponse);
                    this.send(message);
                }))
                .addDisposable(this.raftTransport.sender().voteRequests().subscribe(raftVoteRequest -> {
                    var message = this.gridOpSerDe.serializeRaftVoteRequest(raftVoteRequest);
                    this.send(message);
                }))
                .addDisposable(this.raftTransport.sender().voteResponse().subscribe(raftVoteResponse -> {
                    var message = this.gridOpSerDe.serializeRaftVoteResponse(raftVoteResponse);
                    this.send(message);
                }))
                .addDisposable(this.discovery.events().remoteEndpointJoined().subscribe(remoteEndpointId -> {
                    this.raft.addPeerId(remoteEndpointId);
                    if (this.discovery.getRemoteEndpointIds().size() == 1) {
                        this.raft.start();
                    }
                }))
                .addDisposable(this.discovery.events().remoteEndpointDetached().subscribe(remoteEndpointId -> {
                    this.raft.removePeerId(remoteEndpointId);
                    if (this.discovery.getRemoteEndpointIds().size() == 0) {
                        this.raft.stop();
                    }
                }))
                .addDisposable(this.discovery.endpointStatesNotifications().subscribe(notification -> {
                    var message = this.gridOpSerDe.serializeEndpointStatesNotification(notification);
                    this.send(message);
                }))
                .addDisposable(this.discovery.helloNotifications().subscribe(notification -> {
                    var message = this.gridOpSerDe.serializeHelloNotification(notification);
                    this.send(message);
                }))
                .addDisposable(this.raft.changedLeaderId().subscribe(newLeaderId -> {
                    if (newLeaderId.isEmpty()) {
                        this.discovery.stopSendingEndpointStates();
                    } else if (newLeaderId.get() != this.config.localEndpointId()) {
                        this.discovery.startSendingEndpointStates();
                    } else if (!this.discovery.isSendingEndpointStates()) {
                        this.discovery.startSendingEndpointStates();
                    }
                }))
                .addDisposable(this.raft.committedEntries().subscribe(logEntry -> {
                    // a commit should go to the upper layer.
                    // if it is not the leader then a request must be converted to notification,
                    // and it must be because when a new follower later join
                    // it should not be replied to a previous request
                    // therefore the request is responded by one and only one member of the grid, the leader
                    var message = this.messageCodec.decode(logEntry.entry());
                    if (this.getLocalEndpointId() == this.getLeaderId()) {
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
                .addDisposable(this.raft.commitIndexSyncRequests().subscribe(signal -> {
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
                            var boundStorage = this.storages.get(storageId);
                            if (boundStorage == null) {
                                logger.warn("Not found storage for storageSync response {}", storageId);
                                continue;
                            }
                            var storage = boundStorage.storage();
                            if (storage == null) {
                                logger.warn("Storage with id {} not found for sync", storageId);
                                continue;
                            }
                            if (storage.getClass().getSimpleName().equals(ReplicatedStorage.class.getSimpleName())) {
                                Message updateNotificationMessage = null;
                                try {
                                    updateNotificationMessage = this.messageCodec.decode(updateNotification);
                                } catch (Throwable e) {
                                    logger.warn("Update notification deserialization failed", e);
                                    continue;
                                }
                                // Let's Rock
                                var keys = storage.keys();
                                storage.evictAll(keys);
                                this.dispatch(updateNotificationMessage);
                                logger.info("Storage {} is synchronized with the leader.", storage.getId());
                            }
                        }
                        this.raft.setCommitIndex(response.commitIndex());
                    });
                }))
                .onCompleted(() -> {
                    logger.info("Disposed");
                })
                .build();
        this.discovery.startSendingHelloNotifications();
    }

    private void dispatch(Message message) {
        var type = MessageType.valueOfOrNull(message.type);
        if (type == null) {
            logger.warn("Unrecognized message received {}", JsonUtils.objectToString(message));
            return;
        }
        switch(type) {
            case HELLO_NOTIFICATION -> {
                var notification = this.gridOpSerDe.deserializeHelloNotification(message);
                this.discovery.acceptHelloNotification(notification);
            }
            case ENDPOINT_STATES_NOTIFICATION -> {
                var notification = this.gridOpSerDe.deserializeEndpointStatesNotification(message);
                this.discovery.acceptEndpointStatesNotification(notification);
            }
            case RAFT_APPEND_ENTRIES_REQUEST -> {
                var appendEntriesRequest = this.gridOpSerDe.deserializeRaftAppendRequest(message);
                this.raftTransport.receiver().appendEntriesRequest().onNext(appendEntriesRequest);
            }
            case RAFT_APPEND_ENTRIES_RESPONSE -> {
                var appendEntriesResponse = this.gridOpSerDe.deserializeRaftAppendResponse(message);
                this.raftTransport.receiver().appendEntriesResponse().onNext(appendEntriesResponse);
            }
            case RAFT_VOTE_REQUEST -> {
                var voteRequest = this.gridOpSerDe.deserializeRaftVoteRequest(message);
                this.raftTransport.receiver().voteRequests().onNext(voteRequest);
            }
            case RAFT_VOTE_RESPONSE -> {
                var voteResponse = this.gridOpSerDe.deserializeRaftVoteResponse(message);
                this.raftTransport.receiver().voteResponse().onNext(voteResponse);
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
                int savedCommitIndex = this.raft.getCommitIndex();
                for (var it = this.storages.values().iterator(); it.hasNext(); ) {
                    var boundStorage = it.next();
                    var storage = boundStorage.storage();
                    if (storage == null) continue; // not ready
                    if (storage.getClass().getSimpleName().equals(ReplicatedStorage.class.getSimpleName())) {
                        var keys = storage.keys();
                        var entries = storage.getAll(keys);
                        StorageEndpoint endpoint = (StorageEndpoint) boundStorage.endpoints().stream().findFirst().get();
                        var updateNotification = new UpdateEntriesNotification(entries, this.getLocalEndpointId(), request.sourceEndpointId());
                        var updateNotificationMessage = endpoint.messageSerDe.serializeUpdateEntriesNotification(updateNotification);
                        byte[] storageEndpointNotification;
                        try {
                            storageEndpointNotification = this.messageCodec.encode(updateNotificationMessage);
                        } catch (Throwable e) {
                            logger.warn("Error while serializing message", e);
                            continue;
                        }
                        storageUpdateNotifications.put(storage.getId(), storageEndpointNotification);
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
                    logger.info("Sending Storage sync again to another leader. prev assumed leader: {}, the leader we send now: {}", message.sourceId, response.leaderId());
                    var newRequest = new StorageSyncRequest(response.requestId(), response.leaderId());
                    var newMessage = this.gridOpSerDe.serializeStorageSyncRequest(newRequest);
                    this.send(newMessage);
                    return;
                }
                var pendingRequest = this.pendingRequests.get(response.requestId());
                if (pendingRequest == null) {
                    logger.warn("There is no Storage Sync pending request for requestId {}", response.requestId());
                    return;
                }
                pendingRequest.complete(message);
            }
            case SUBMIT_REQUEST -> {
                var request = this.gridOpSerDe.deserializeSubmitRequest(message);
                var response = this.gridOpSerDe.serializeSubmitResponse(request.createResponse(
                        message.sourceId,
                        this.raft.submit(request.entry()) != null,
                        this.raft.getLeaderId()
                ));
                this.send(response);
            }
            case SUBMIT_RESPONSE -> {
                var requestId = message.requestId;
                if (requestId == null) {
                    logger.warn("No requestId attached for response {}", JsonUtils.objectToString(message));
                    return;
                }
                var pendingRequest = this.pendingRequests.get(requestId);
                if (pendingRequest == null) {
                    logger.warn("No pending request is registered for response {}", JsonUtils.objectToString(message));
                    return;
                }
                pendingRequest.complete(message);
            }
            default -> {
                var storageId = message.storageId;
                if (storageId == null) {
                    logger.warn("No StorageId is defined for message {}", JsonUtils.objectToString(message));
                    return;
                }
                CorrespondedStorage correspondedStorage = this.storages.get(storageId);
                if (correspondedStorage == null) {
                    logger.warn("No message acceptor for storage {}", storageId);
                    return;
                }
                List<StorageEndpoint> endpoints = correspondedStorage.endpoints();
                endpoints.forEach(endpoint -> endpoint.receive(message));
            }
        }
    }

    public<K, V> SeparatedStorageBuilder<K, V> separatedStorage() {
        var storageGrid = this;
        var storageGroup = SeparatedStorageBuilder.class.getSimpleName();
        return new SeparatedStorageBuilder<K, V>()
                .setMapDepotProvider(MapUtils::makeMapAssignerDepot)
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    this.addMessageAcceptor(storageEndpoint.getStorageId(), storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    var updatedBoundStorage = this.storages.getOrDefault(storage.getId(), CorrespondedStorage.createEmpty())
                                    .setStorage(storage);
                    this.storages.put(storage.getId(), updatedBoundStorage);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.storages::remove);
                })
                ;
    }

    public<K, V> FederatedStorageBuilder<K, V> federatedStorage() {
        var storageGrid = this;
        return new FederatedStorageBuilder<K, V>()
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    this.addMessageAcceptor(storageEndpoint.getStorageId(), storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    var updatedBoundStorage = this.storages.getOrDefault(storage.getId(), CorrespondedStorage.createEmpty())
                            .setStorage(storage);
                    this.storages.put(storage.getId(), updatedBoundStorage);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.storages::remove);
                })
                ;
    }

    public<K, V> ReplicatedStorageBuilder<K, V> replicatedStorage() {
        var storageGrid = this;
        return new ReplicatedStorageBuilder<K, V>()
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    this.addMessageAcceptor(storageEndpoint.getStorageId(), storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    var updatedBoundStorage = this.storages.getOrDefault(storage.getId(), CorrespondedStorage.createEmpty())
                            .setStorage(storage);
                    this.storages.put(storage.getId(), updatedBoundStorage);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.storages::remove);
                })
                ;
    }


    public StorageGridTransport transport() {
        return StorageGridTransport.create(this.receiver, this.sender);
    }

    Set<UUID> getRemoteEndpointIds() {
        return this.discovery.getRemoteEndpointIds();
    }

    int getRequestTimeoutInMs() {
        return this.config.requestTimeoutInMs();
    }

    void send(Message message) {
        byte[] bytes;
        try {
            message.sourceId = this.config.localEndpointId();
            bytes = this.messageCodec.encode(message);
        } catch (Throwable e) {
            logger.warn("Cannot encode message {}", JsonUtils.objectToString(message), e);
            return;
        }
        this.sender.onNext(bytes);
    }

    void submit(Message message) {
        var leaderId = this.raft.getLeaderId();
        if (leaderId == null) {
            var waitForLeader = new CompletableFuture<Optional<UUID>>();
            this.raft.changedLeaderId().firstElement().subscribe(waitForLeader::complete);
            try {
                var maybeNewLeaderId = waitForLeader.get(3000, TimeUnit.MILLISECONDS);
                leaderId = maybeNewLeaderId.orElse(null);
            } catch(Exception e) {
                logger.warn("Exception occurred while waiting for leaders", e);
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
            logger.warn("Error occurred while encoding entry", e);
            this.submit(message);
            return;
        }
        if (leaderId == this.config.localEndpointId()) {
            // we can submit here.
            if (this.raft.submit(entry) == null) {
                logger.warn("Lead submit returned with null. retry");
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
            logger.warn("Error occurred while waiting for submit response", e);
        } finally {
            this.pendingRequests.remove(request.requestId);
        }
    }

    Observable<UUID> joinedRemoteEndpoints() {
        return this.discovery.events().remoteEndpointJoined();
    }

    Observable<UUID> detachedRemoteEndpoints() {
        return this.discovery.events().remoteEndpointDetached();
    }

    Observable<Void> inactivatedLocalEndpoint() {
        return this.discovery.events()
                .localEndpointInactivated()
                .debounce(500, TimeUnit.MILLISECONDS, Schedulers.computation());
    }

    Observable<Optional<UUID>> changedLeaderId() {
        return this.raft.changedLeaderId();
    }

    UUID getLeaderId() {
        return this.raft.getLeaderId();
    }

    UUID getLocalEndpointId() {
        return this.config.localEndpointId();
    }

    private void addMessageAcceptor(String storageId, StorageEndpoint endpoint) {
        var updatedBindableStorage = this.storages
                .getOrDefault(storageId, CorrespondedStorage.createEmpty())
                .addEndpoint(endpoint);
        this.storages.put(storageId, updatedBindableStorage);
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
