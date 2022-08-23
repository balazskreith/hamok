package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.HamokError;
import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.Raccoon;
import io.github.balazskreith.hamok.raccoons.events.HelloNotification;
import io.github.balazskreith.hamok.storagegrid.messages.GridOpSerDe;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.storagegrid.messages.MessageType;
import io.github.balazskreith.hamok.storagegrid.messages.SubmitRequest;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class StorageGrid implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(StorageGrid.class);

    public static StorageGridBuilder builder() {
        return new StorageGridBuilder();
    }

    private final AtomicReference<CompletableFuture<Boolean>> pendingSync = new AtomicReference<>(null);
    private final Map<UUID, SubmitRequest> pendingSubmits;

    private final Map<String, GridActor> actors = new ConcurrentHashMap<>();
    private final GridOpSerDe gridOpSerDe;
    private final Subject<Message> sender = PublishSubject.<Message>create().toSerialized();
    private final Subject<Message> receiver = PublishSubject.<Message>create().toSerialized();
    private final Scheduler submissionScheduler;
    private final Subject<HamokError> errors = PublishSubject.create();

    private final Disposer disposer;
    private final StorageGridConfig config;
    private final StorageGridMetrics metrics = new StorageGridMetrics();

    private final StorageGridExecutors executors;
    private final Raccoon raccoon;
    private final String context;
    private final StorageGridTransport transport;

    StorageGrid(
            StorageGridConfig config,
            StorageGridExecutors executors,
            Raccoon raccoon,
            GridOpSerDe gridOpSerDe,
            String context
    ) {
        this.config = config;
        this.executors = executors;
        this.raccoon = raccoon;
        this.gridOpSerDe = gridOpSerDe;
        this.context = context;
        this.submissionScheduler = Schedulers.from(this.executors.getSubmissionExecutor());
        this.pendingSubmits = new ConcurrentHashMap<>();
        var observableSending = this.sender.observeOn(Schedulers.from(this.executors.getSendingMessageExecutor()));
        this.transport = StorageGridTransport.create(this.receiver, observableSending);
        var observableReceiving = this.receiver;
        this.disposer = Disposer.builder()
                .addDisposable(this.raccoon)
                .addSubject(this.sender)
                .addSubject(this.receiver)
                .addDisposable(observableReceiving.subscribe(message -> {
                    logger.trace("{} received message (type: {}) from {}", this.getLocalEndpointId().toString().substring(0, 8), message.type, message.sourceId.toString().substring(0, 8));
                    if (UuidTools.equals(message.sourceId, this.getLocalEndpointId())) {
                        // multicast dispatch to everywhere, but
                        // packets sent to wire should not be received on loopback
                        return;
                    }
                    if (message.destinationId == null || UuidTools.equals(message.destinationId, this.getLocalEndpointId())) {
                        this.dispatch(message);
                    } else {
//                        logger.warn("Message destination {} is not for this local endpoint {} {}", message.destinationId, this.getLocalEndpointId());
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
                    logger.debug("{} Committed message is received (leader: {}). commitIndex {}. Message: {}", this.getLocalEndpointId(), this.raccoon.getLeaderId(), logEntry.index(), logEntry.entry());
                    this.dispatch(logEntry.entry());
                }))
                .addDisposable(this.raccoon.syncRequests().subscribe(signal -> {
                    // this cannot be requested by the leader only the follower, so
                    // we are "sure" that our local endpoint is not the leader endpoint.
                    try {
                        var synced = this.executeSync();
                        synced.thenApply(signal::complete);
                    } catch (Throwable t) {
                        this.triggerHamokError(HamokError.FAILED_SYNC, t);
                    }
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
        logger.debug("{} ({}) received message type {} ", this.getLocalEndpointId(), this.context, message.type);
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
            case SUBMIT_REQUEST -> {
                var request = this.gridOpSerDe.deserializeSubmitRequest(message);
                this.submissionScheduler.scheduleDirect(() -> {
                    var submittedMessage = request.entry();
                    var success = this.raccoon.submit(submittedMessage);
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
                var submission = this.pendingSubmits.remove(requestId);
                if (submission == null) {
                    logger.warn("No submission was registered for id {}", submission.requestId());
                }
            }
            default -> {
                var storageId = message.storageId;
                if (storageId == null) {
                    logger.warn("{} No StorageId is defined for message {}", this.context, message);
                    return;
                }
                var member = this.actors.get(storageId);
                if (member == null) {
                    logger.warn("{} No message acceptor for storage {}. Message: {}", this.context, storageId, message);
                    return;
                }
                member.accept(message);
            }
        }
    }

    public <K, V> SeparatedStorageBuilder<K, V> separatedStorage() {
        return separatedStorage(null);
    }

    public <K, V> SeparatedStorageBuilder<K, V> separatedStorage(Storage<K, V> baseStorage) {
        var storageGrid = this;
        var operationsBuilder = DistributedStorageOperations.<K, V>builder();
        return new SeparatedStorageBuilder<K, V>()
                .setStorage(baseStorage)
                .setMapDepotProvider(MapUtils::makeMapAssignerDepot)
                .setStorageGrid(storageGrid)
                .onEndpointBuilt(storageEndpoint -> {
                    operationsBuilder.withEndpoint(storageEndpoint);
                })
                .onStorageBuilt(storage -> {
                    var gridMember = SeparatedStorage.createGridMember(storage);
                    this.actors.put(gridMember.getIdentifier(), gridMember);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.actors::remove);
                    logger.info("Created Separated Storage {} on StorageGrid ()", gridMember.getIdentifier(), this.context);
                })
                ;
    }

    public <K, V> PropagatedCollectionsBuilder<K, V, Set<V>> propagatedSets() {
        var storageGrid = this;
        return new PropagatedCollectionsBuilder<K, V, Set<V>>()
                .setStorageGrid(storageGrid)
                .setNewCollectionSupplier(HashSet::new)
                .setMapDepotProvider(MapUtils::makeMapAssignerDepot)
                .onPropagatedCollectionsBuilt(propagatedCollections -> {
                    var gridMember = PropagatedCollections.createGridMember(propagatedCollections);
                    this.actors.put(gridMember.getIdentifier(), gridMember);
                    propagatedCollections.events().closingStorage()
                            .firstElement().subscribe(this.actors::remove);
                    logger.info("Created Propagated Collection {} on StorageGrid ()", gridMember.getIdentifier(), this.context);
                })
                ;
    }

    public <K, V> ReplicatedStorageBuilder<K, V> replicatedStorage() {
        return replicatedStorage(null);
    }

    public <K, V> ReplicatedStorageBuilder<K, V> replicatedStorage(Storage<K, V> baseStorage) {
        var storageGrid = this;
        return new ReplicatedStorageBuilder<K, V>()
                .setStorage(baseStorage)
                .setStorageGrid(storageGrid)
                .setMapDepotProvider(MapUtils::makeMapAssignerDepot)
                .onStorageBuilt(storage -> {
                    var gridMember = ReplicatedStorage.createGridMember(storage);
                    this.actors.put(gridMember.getIdentifier(), gridMember);
                    storage.events().closingStorage()
                            .firstElement().subscribe(this.actors::remove);
                    logger.info("Created Replicatd Storage {} on StorageGrid ()", gridMember.getIdentifier(), this.context);
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

    void submit(Message message) {
        message.sourceId = this.getLocalEndpointId();
//        logger.info("{} submit a message {}", this.getLocalEndpointId(), JsonUtils.objectToString(message));
        var leaderId = this.raccoon.getLeaderId();
        if (leaderId == null) {
            var waitForLeader = new CompletableFuture<Optional<UUID>>();
            this.raccoon.changedLeaderId().firstElement().subscribe(waitForLeader::complete);
            try {
                var maybeNewLeaderId = waitForLeader.get(3000, TimeUnit.MILLISECONDS);
                leaderId = maybeNewLeaderId.orElse(this.raccoon.getLeaderId());
            } catch (Exception e) {
                logger.warn("{} Exception occurred while waiting for leaders", this.context, e);
            }

            if (leaderId == null) {
                this.submit(message);
            }
        }
        var localEndpointId =  this.raccoon.getId();
        if (UuidTools.equals(leaderId, localEndpointId)) {
            logger.debug("{} submitted message is directly routed to the leader", this.getLeaderId());
            // we can submit here.
            this.raccoon.submit(message);
            return;
        }
        // we need to send it to the leader in an embedded message
        var submission = new SubmitRequest(UUID.randomUUID(), leaderId, message);
        this.pendingSubmits.put(submission.requestId(), submission);
        var request = this.gridOpSerDe.serializeSubmitRequest(submission);
        this.send(request);
    }

    /**
     * Make the grid to be sync. if no other endpoint is added then
     * this just returns. if there are available endpoints then this method request a storage sync
     */
    public void sync() throws ExecutionException, InterruptedException, TimeoutException {
        var remoteEndpointIds = this.raccoon.getRemoteEndpointIds();
        if (remoteEndpointIds.size() < 1) {
            return;
        }
        if (this.getLeaderId() == null) {
            var holder = new AtomicReference<Disposable>();
            var wait = new CompletableFuture<Disposable>();
            wait.thenAccept(disposable -> {
                disposable.dispose();
            });
            var d = this.changedLeaderId().subscribe(o -> {
                if (o.isEmpty()) return;
                wait.complete(holder.get());
            });
            holder.set(d);
            wait.get();
        }
        this.executeSync().get();
    }

    private CompletableFuture<Boolean> executeSync() throws TimeoutException, ExecutionException, InterruptedException {
        var result = new CompletableFuture<Boolean>();
        if (!this.pendingSync.compareAndSet(null, result)) {
            return this.pendingSync.get();
        }
        this.executors.getSyncOperationExecutor().submit(() -> {
            var successes = new HashSet<>();
            for (int attempt = 0; attempt < 3; ++attempt) {
                for (var actor : this.actors.values()) {
                    var actorId = actor.getIdentifier();
                    if (successes.contains(actorId)) {
                        continue;
                    }
                    logger.info("Start executing sync {}", actorId);
                    var synced = actor.executeSync();
                    logger.info("Executed sync {} success: {}", actorId, synced);
                    if (synced) {
                        successes.add(actorId);
                    } else {
                        logger.warn("Unsuccessful synchronization attempt for {}. ", actor.getIdentifier());
                    }
                }
                if (successes.size() < this.actors.size()) {
                    continue;
                }
                break;
            }
            this.pendingSync.set(null);
            var success = this.actors.size() <= successes.size();
            result.complete(success);
        });
        return result;
    }

    private void triggerHamokError(int code, Throwable exception) {
        if (!this.errors.hasObservers()) {
            throw new RuntimeException("Error occurred in operation. code: " + code, exception);
        }
        var error = HamokError.create(code, exception);
        this.errors.onNext(error);
    }

    public Observable<UUID> joinedRemoteEndpoints() {
        return this.raccoon.joinedRemotePeerId();
    }

    public Observable<UUID> detachedRemoteEndpoints() {
        return this.raccoon.detachedRemotePeerId();
    }

    public Observable<HamokError> errors() {
        return this.errors;
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

    StorageGridExecutors executors() {
        return this.executors;
    }

    public UUID getLeaderId() {
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
