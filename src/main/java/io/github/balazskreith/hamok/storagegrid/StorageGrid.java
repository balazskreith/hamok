package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.CompletablePromises;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.Raccoon;
import io.github.balazskreith.hamok.raccoons.events.HelloNotification;
import io.github.balazskreith.hamok.storagegrid.messages.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StorageGrid implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(StorageGrid.class);

    public static StorageGridBuilder builder() {
        return new StorageGridBuilder();
    }

//    private final AtomicReference<Subject<Message>> pendingStorageSyncs = new AtomicReference<>(null);
    private final AtomicReference<StorageSyncOperation> pendingStorageSyncs = new AtomicReference<>(null);
    private final CompletablePromises<UUID, Boolean> pendingSubmits;
    private final Map<String, DistributedStorageOperations> storageOperations = new ConcurrentHashMap<>();
    private final GridOpSerDe gridOpSerDe;
    private final Subject<Message> sender = PublishSubject.<Message>create().toSerialized();
    private final Subject<Message> receiver = PublishSubject.<Message>create().toSerialized();

    private final Disposer disposer;
    private final StorageGridConfig config;
    private final StorageGridMetrics metrics = new StorageGridMetrics();

    private final Raccoon raccoon;
    private final String context;
    private final StorageGridTransport transport;

    StorageGrid(
            StorageGridConfig config,
            Raccoon raccoon,
            GridOpSerDe gridOpSerDe,
            String context
    ) {
        this.config = config;
        this.raccoon = raccoon;
        this.gridOpSerDe = gridOpSerDe;
        this.context = context;
        this.pendingSubmits = new CompletablePromises<UUID, Boolean>(this.config.requestTimeoutInMs(), "Pending Submission Requests");
        this.transport = StorageGridTransport.create(this.receiver, this.sender);

        this.disposer = Disposer.builder()
                .addDisposable(this.raccoon)
                .addSubject(this.sender)
                .addSubject(this.receiver)
                .addDisposable(this.receiver.subscribe(message -> {
                    logger.trace("{} received message (type: {}) from {}", this.getLocalEndpointId().toString().substring(0, 8), message.type, message.sourceId.toString().substring(0, 8));
                    if (UuidTools.equals(message.sourceId, this.getLocalEndpointId())) {
                        // multicast dispatch to everywhere, but
                        // packets sent to wire should not be received on loopback
                        return;
                    }
                    if (message.destinationId == null || UuidTools.equals(message.destinationId, this.getLocalEndpointId())) {
                        this.dispatch(message);
                    } else {
                        logger.warn("Message destination {} is not for this local endpoint {} {}", message.destinationId, this.getLocalEndpointId());
                    }

                }))
                .addDisposable(this.raccoon.outboundEvents().appendEntriesRequest().subscribe(raftAppendEntriesRequest -> {
                    var message = this.gridOpSerDe.serializeRaftAppendRequestChunk(raftAppendEntriesRequest);
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
                    logger.info("{} ({}) is informed that the leader is changed to: {}", this.getLocalEndpointId(), this.context, newLeaderId);
                }))
                .addDisposable(this.raccoon.committedEntries().subscribe(logEntry -> {
                    // message is committed to the quorum of the cluster, so we can dispatch it
                    logger.trace("{} Committed message is received (leader: {}). commitIndex {}. Message: ", this.getLocalEndpointId(), this.raccoon.getLeaderId(), logEntry.index(), logEntry.entry());
                    this.dispatch(logEntry.entry());
                }))
                .addDisposable(this.raccoon.commitIndexSyncRequests().subscribe(signal -> {

                    // this cannot be requested by the leader only the follower, so
                    // we are "sure" that our local endpoint is not the leader endpoint.
                    UUID requestId = UUID.randomUUID();
                    var request = new StorageSyncRequest(requestId, this.getLocalEndpointId(), this.getLeaderId());
                    var operation = StorageSyncOperation.builder()
                            .setRequestId(requestId)
                            .setDecoder(this.gridOpSerDe::deserializeStorageSyncResponse)
                            .setCleanLocalStorageConsumer(storageId -> {
                                var storageOperations = this.storageOperations.get(storageId);
                                if (storageOperations == null) {
                                    logger.warn("{} Not found storage for storageSync response {}", this.context, storageId);
                                    return;
                                }
                                storageOperations.storageLocalClear();
                                logger.warn("Storage {} is cleaned", storageId);
                            })
                            .setProcessNotificationConsumer(this::dispatch)
                            .build();
                    if (!this.pendingStorageSyncs.compareAndSet(null, operation)) {
                        logger.warn("Concurrent storage sync occurred. only one is going to be executed");
                        return;
                    }
                    operation.onCompleted(newCommitIndex -> {
                        this.raccoon.setCommitIndex(newCommitIndex);
                        this.pendingStorageSyncs.set(null);
                    }).onFailed(err -> {
                        logger.warn("Error occurred while syncing storages", err);
                        this.pendingStorageSyncs.set(null);
                    });
                    var message = this.gridOpSerDe.serializeStorageSyncRequest(request);
                    this.send(message);
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
            logger.warn("{} ({}) received an unrecognized message {}", this.getLocalEndpointId(), this.context, message);
            return;
        }
        logger.trace("{} ({}) received message {}", this.getLocalEndpointId(), this.context, message);
//        if (UuidTools.equals(this.getLocalEndpointId(), message.sourceId)) {
//            logger.warn("Self Addressed message?", message);
//        }
        switch (type) {
            case HELLO_NOTIFICATION -> {
                var notification = this.gridOpSerDe.deserializeHelloNotification(message);
                this.raccoon.inboundEvents().helloNotifications().onNext(notification);
            }
            case ENDPOINT_STATES_NOTIFICATION -> {
                var notification = this.gridOpSerDe.deserializeEndpointStatesNotification(message);
                this.raccoon.inboundEvents().endpointStateNotifications().onNext(notification);
            }
            case RAFT_APPEND_ENTRIES_REQUEST_CHUNK -> {
                var appendEntriesRequest = this.gridOpSerDe.deserializeRaftAppendRequestChunk(message);
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
                            this.getLeaderId(),
                            -1,
                            true
                    );
                    var responseMessage = this.gridOpSerDe.serializeStorageSyncResponse(response);
                    this.send(responseMessage);
                    return;
                }
                Map<String, Message> storageUpdateNotifications = new HashMap<>();
                int sequence = 0;
                int savedCommitIndex = this.raccoon.getCommitIndex();
                for (var it = this.storageOperations.values().iterator(); it.hasNext(); ) {
                    var storageOperations = it.next();
                    if (storageOperations == null) continue; // not ready
                    if (storageOperations.getStorageClassSimpleName().equals(ReplicatedStorage.class.getSimpleName())) {
                        var jt = storageOperations.getAllKeysUpdateNotification(request.sourceEndpointId());
                        for (; jt.hasNext(); ++sequence) {
                            var response = request.createResponse(
                                    savedCommitIndex,
                                    Map.of(storageOperations.getStorageId(), jt.next()),
                                    true,
                                    this.getLeaderId(),
                                    sequence,
                                    false
                            );
                            var respondingMessage = this.gridOpSerDe.serializeStorageSyncResponse(response);
                            this.send(respondingMessage);
                        }
                    }
                }

                var response = request.createResponse(
                        savedCommitIndex,
                        storageUpdateNotifications,
                        true,
                        this.getLeaderId(),
                        ++sequence,
                        true
                );
                var respondingMessage = this.gridOpSerDe.serializeStorageSyncResponse(response);
                this.send(respondingMessage);
            }
            case STORAGE_SYNC_RESPONSE -> {
                var response = this.gridOpSerDe.deserializeStorageSyncResponse(message);
                if (!response.success()) {
                    logger.info("{} Sending Storage sync again to another leader. prev assumed leader: {}, the leader we send now: {}", this.context, message.sourceId, response.leaderId());
                    var newRequest = new StorageSyncRequest(response.requestId(), this.getLocalEndpointId(), response.leaderId());
                    var newMessage = this.gridOpSerDe.serializeStorageSyncRequest(newRequest);
                    this.send(newMessage);
                    return;
                }
                var pendingSync = this.pendingStorageSyncs.get();
                if (pendingSync == null) {
                    logger.warn("{} There is no Storage Sync pending request for requestId {}", this.context, response.requestId());
                    return;
                }
                pendingSync.process(message);
            }
            case SUBMIT_REQUEST -> {
                var request = this.gridOpSerDe.deserializeSubmitRequest(message);
                Schedulers.io().scheduleDirect(() -> {
                        var success = this.raccoon.submit(request.entry());
                        logger.debug("{} Creating response for commit {}", this.getLocalEndpointId(), success);
                        var response = this.gridOpSerDe.serializeSubmitResponse(request.createResponse(
                                message.sourceId,
                                success,
                                this.raccoon.getLeaderId()
                        ));
                        this.send(response);
                });
            }
            case SUBMIT_RESPONSE -> {
                var requestId = message.requestId;
                if (requestId == null) {
                    logger.warn("{} No requestId attached for response {}", this.context, message);
                    return;
                }
                this.pendingSubmits.resolve(requestId, message.success);
            }
            default -> {
                var storageId = message.storageId;
                if (storageId == null) {
                    logger.warn("{} No StorageId is defined for message {}", this.context, message);
                    return;
                }
                var storageOperations = this.storageOperations.get(storageId);
                if (storageOperations == null) {
                    logger.warn("{} No message acceptor for storage {}. Message: {}", this.context, storageId, message);
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
                    int maxMessageEntries = Math.min(storage.getConfig().maxMessageKeys(), storage.getConfig().maxMessageValues());
                    var operations = operationsBuilder
                            .withMaxMessageEntries(maxMessageEntries)
                            .withStorage(storage).build();
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
                    int maxMessageEntries = Math.min(storage.getConfig().maxMessageKeys(), storage.getConfig().maxMessageValues());
                    var operations = operationsBuilder
                            .withMaxMessageEntries(maxMessageEntries)
                            .withStorage(storage).build();
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
                .setMapDepotProvider(MapUtils::makeMapAssignerDepot)
                .onEndpointBuilt(storageEndpoint -> {
                    operationsBuilder.withEndpoint(storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    int maxMessageEntries = Math.min(storage.getConfig().maxMessageKeys(), storage.getConfig().maxMessageValues());
                    var operations = operationsBuilder
                            .withMaxMessageEntries(maxMessageEntries)
                            .withStorage(storage).build();
                    this.storageOperations.put(storage.getId(), operations);
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
        return this.transport;
    }

    public UUID getLocalEndpointId() {
        return this.raccoon.getId();
    }

    Set<UUID> getRemoteEndpointIds() {
        return this.raccoon.getRemoteEndpointIds();
    }

    int getRequestTimeoutInMs() {
        return this.config.requestTimeoutInMs();
    }

    void send(Message message) {
        message.sourceId = this.raccoon.getId();
        logger.trace("{} sending message (type: {}) to {}", this.getLocalEndpointId().toString().substring(0, 8), message.type, message.destinationId);
        if (UuidTools.equals(message.destinationId, this.getLocalEndpointId())) {
            // loopback message
            this.dispatch(message);
            return;
        }
        this.sender.onNext(message);
    }

    boolean submit(Message message) {
        message.sourceId = this.getLocalEndpointId();
//        logger.info("{} submit a message {}", this.getLocalEndpointId(), JsonUtils.objectToString(message));
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
                return this.submit(message);
            }
        }
        var localEndpointId =  this.raccoon.getId();
        if (UuidTools.equals(leaderId, localEndpointId)) {
            logger.debug("{} submitted message is directly routed to the leader", this.getLeaderId());
            // we can submit here.
            var submitted = this.raccoon.submit(message);
            return submitted;
        }
        // we need to send it to the leader in an embedded message
        var requestId = UUID.randomUUID();
        var pendingSubmit = this.pendingSubmits.create(requestId);
        var record = new SubmitRequest(requestId, leaderId, message);
        var request = this.gridOpSerDe.serializeSubmitRequest(record);

        this.send(request);
        var success = this.pendingSubmits.await(requestId).orElse(false);
        return success;
    }

    public Observable<UUID> joinedRemoteEndpoints() {
        return this.raccoon.joinedRemotePeerId();
    }

    public Observable<UUID> detachedRemoteEndpoints() {
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

    @Override
    public void dispose() {
        this.disposer.dispose();
    }

    @Override
    public boolean isDisposed() {
        return this.disposer.isDisposed();
    }


}
