package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.HamokError;
import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.Utils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.Raccoon;
import io.github.balazskreith.hamok.raccoons.events.HelloNotification;
import io.github.balazskreith.hamok.storagegrid.messages.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Creates an instance represents the Storage grid in the cluster
 */
public class StorageGrid implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(StorageGrid.class);

    public static StorageGridBuilder builder() {
        return new StorageGridBuilder();
    }

    public enum State {
        RUN,
        SYNCING,
        CLOSED
    }

    private final AtomicReference<CompletableFuture<Boolean>> pendingSync = new AtomicReference<>(null);
    private final Map<UUID, CompletableFuture<StorageSyncResponse>> pendingStorageSyncRequests = new ConcurrentHashMap<>();

    private final Map<String, StorageInGrid> storages = new ConcurrentHashMap<>();
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

    private final Endpoints endpoints;
    private final Events events;
    private final RaftInfo raftInfo;
    private volatile State state = State.RUN;
    private boolean standalone = true;

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
        var observableSending = this.sender.observeOn(Schedulers.from(this.executors.getSendingMessageExecutor()));
        this.transport = StorageGridTransport.create(this.receiver, observableSending);
        var observableReceiving = this.receiver;
        this.events = new Events(
                this.raccoon.changedLeaderId(),
                this.raccoon.joinedRemotePeerId(),
                this.raccoon.detachedRemotePeerId()
        );
        this.endpoints = new Endpoints() {
            @Override
            public UUID getLocalEndpointId() {
                return raccoon.getId();
            }

            @Override
            public Set<UUID> getRemoteEndpointIds() {
                return raccoon.getRemoteEndpointIds();
            }

            @Override
            public UUID getLeaderEndpointId() {
                return raccoon.getLeaderId();
            }
        };
        this.raftInfo = new RaftInfo() {
            @Override
            public int getCommitIndex() {
                return raccoon.getCommitIndex();
            }
        };
        this.disposer = Disposer.builder()
                .addDisposable(this.raccoon)
                .addSubject(this.sender)
                .addSubject(this.receiver)
                .addDisposable(observableReceiving.subscribe(message -> {
                    if (State.CLOSED.equals(this.state)) {
                        logger.warn("StorageGrid ({}) is received a message after it is closed", this.context);
                        return;
                    }
                    logger.trace("{} received message (type: {}) from {}", this.endpoints.getLocalEndpointId().toString().substring(0, 8), message.type, message.sourceId.toString().substring(0, 8));
                    if (UuidTools.equals(message.sourceId, this.endpoints.getLocalEndpointId())) {
                        // multicast dispatch to everywhere, but
                        // packets sent to wire should not be received on loopback
                        return;
                    }
                    if (message.destinationId == null || UuidTools.equals(message.destinationId, this.endpoints.getLocalEndpointId())) {
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
                    this.standalone = this.endpoints.getRemoteEndpointIds().size() < 1;
                }))
                .addDisposable(this.raccoon.outboundEvents().endpointStateNotifications().subscribe(notification -> {
                    var message = this.gridOpSerDe.serializeEndpointStatesNotification(notification);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.outboundEvents().helloNotifications().subscribe(notification -> {
                    var leaderId = this.endpoints.getLeaderEndpointId();
                    if (leaderId != null) {
                        notification = new HelloNotification(notification.sourcePeerId(), leaderId);
                    }
                    var message = this.gridOpSerDe.serializeHelloNotification(notification);
                    this.send(message);
                }))
                .addDisposable(this.raccoon.changedLeaderId().subscribe(newLeaderId -> {
                    logger.info("{} ({}) is informed that the leader is changed to: {}", this.endpoints.getLocalEndpointId(), this.context, newLeaderId);
                    this.standalone = false;
                    if (newLeaderId.isPresent()) {
                        this.sync(120 * 1000);
                    }
                }))
                .addDisposable(this.raccoon.committedEntries().subscribe(logEntry -> {
                    // message is committed to the quorum of the cluster, so we can dispatch it
                    logger.debug("{} Committed message is received (leader: {}). commitIndex {}. Message: {}", this.endpoints.getLocalEndpointId(), this.raccoon.getLeaderId(), logEntry.index(), logEntry.entry());
                    this.dispatch(logEntry.entry());
                }))
                .addDisposable(this.raccoon.syncRequests().subscribe(signal -> {
                    try {
                        var synced = this.executeSync(-1, false);
                        synced.thenApply(signal::complete);
                    } catch (Throwable t) {
                        logger.error("Error occurred while syncing", t);
                    }
                }))
                .onCompleted(() -> {
                    logger.info("{} ({}) is disposed", this.endpoints.getLocalEndpointId(), this.getContext());
                })
                .build();
        logger.info("{} ({}) is created", this.endpoints.getLocalEndpointId(), this.getContext());
        this.raccoon.start();
    }

    private void dispatch(Message message) {
        var type = MessageType.valueOfOrNull(message.type);
        if (type == null) {
            logger.warn("{} ({}) received an unrecognized message {}", this.endpoints.getLocalEndpointId(), this.context, message);
            return;
        }
        this.metrics.incrementReceivedMessages();
        logger.debug("{} ({}) received message type {} ", this.endpoints.getLocalEndpointId(), this.context, message.type);
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
                if (UuidTools.notEquals(this.endpoints.getLocalEndpointId(), this.endpoints.getLeaderEndpointId())) {
                    return;
                }
                // so many junks, only the commitIndex worth anything now
                var response = this.gridOpSerDe.serializeStorageSyncResponse(new StorageSyncResponse(
                        message.requestId,
                        message.sourceId,
                        message.raftLeaderId,
                        this.raccoon.getNumberOfCommits(),
                        this.raccoon.getLastApplied(),
                        this.raccoon.getCommitIndex()
                ));
                this.send(response);
            }
            case STORAGE_SYNC_RESPONSE -> {
                if (message.requestId == null) {
                    logger.warn("Received Message does not have a requestId. {}", message);
                    return;
                }
                var promise = this.pendingStorageSyncRequests.remove(message.requestId);
                if (promise != null) {
                    var storageSyncResponse = this.gridOpSerDe.deserializeStorageSyncResponse(message);
                    promise.complete(storageSyncResponse);
                }
            }
            case SUBMIT_REQUEST -> {
                if (UuidTools.notEquals(this.endpoints.getLocalEndpointId(), this.endpoints.getLeaderEndpointId())) {
                    return;
                }
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
//                var submission = this.pendingSubmits.remove(requestId);
//                if (submission == null) {
//                    logger.warn("No submission was registered for id {}", submission.requestId());
//                }
            }
            default -> {
                var storageId = message.storageId;
                if (storageId == null) {
                    logger.warn("{} No StorageId is defined for message {}", this.context, message);
                    return;
                }
                var member = this.storages.get(storageId);
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
        this.requiredNotToBeClosed();
        var storageGrid = this;
        return new SeparatedStorageBuilder<K, V>()
                .setStorageGrid(storageGrid)
                .setStorage(baseStorage)
                .onStorageInGridReady(storageInGrid -> {
                    storageInGrid.observableClosed().subscribe(storageId -> {
                        this.storages.remove(storageId);
                    });
                    this.storages.put(storageInGrid.getIdentifier(), storageInGrid);
                })
                ;
    }

    /**
     * Creates a builder for a replicated storage.
     *
     * @param <K> the type of the key of the replicated storage
     * @param <V> the type of the value of the replicated storage
     * @return A builder for a storage
     */
    public <K, V> ReplicatedStorageBuilder<K, V> replicatedStorage() {
        return replicatedStorage(null);
    }


    /**
     * Creates a builder for a replicated storage using facade a base storage
     *
     * @param baseStorage the storage used underlying to replicate
     * @param <K> the type of the key of the replicated storage
     * @param <V> the type of the value of the replicated storage
     * @return A builder for a storage
     */
    public <K, V> ReplicatedStorageBuilder<K, V> replicatedStorage(Storage<K, V> baseStorage) {
        this.requiredNotToBeClosed();
        var storageGrid = this;
        return new ReplicatedStorageBuilder<K, V>()
                .setStorage(baseStorage)
                .setStorageGrid(storageGrid)
                .onStorageInGridReady(storageInGrid -> {
                    this.storages.put(storageInGrid.getIdentifier(), storageInGrid);
                    this.metrics.incrementReplicatedStorage();
                    storageInGrid.observableClosed().subscribe(storageId -> {
                        this.storages.remove(storageId);
                        this.metrics.decrementReplicatedStorage();
                    });
                })
                ;
    }

    /**
     * Adds a remote endpoint to the grid.
     *
     * @param endpointId
     */
    public void addRemoteEndpointId(UUID endpointId) {
        this.raccoon.addRemotePeerId(endpointId);
    }

    /**
     * Removes a remote endpoint from the grid
     * @param endpointId
     */
    public void removeRemoteEndpointId(UUID endpointId) {
        this.raccoon.removeRemotePeerId(endpointId);
    }

    /**
     * Access to the transport of the grid
     * @return
     */
    public StorageGridTransport transport() {
        return this.transport;
    }

    public Events events() {
        return this.events;
    }

    public Endpoints endpoints() {
        return this.endpoints;
    }

    public RaftInfo raft() {
        return this.raftInfo;
    }

    public State getState() {
        return this.state;
    }

    int getRequestTimeoutInMs() {
        return this.config.requestTimeoutInMs();
    }

    void send(Message message) {
        message.sourceId = this.raccoon.getId();
        logger.trace("{} sending message (type: {}) to {}", this.endpoints.getLocalEndpointId().toString().substring(0, 8), message.type, message.destinationId);
        if (UuidTools.equals(message.destinationId, this.endpoints.getLocalEndpointId())) {
            // loopback message
            this.dispatch(message);
            return;
        }
        this.metrics.incrementSentMessages();
        this.sender.onNext(message);
    }

    void submit(Message message) {
        message.sourceId = this.endpoints.getLocalEndpointId();
//        logger.info("{} submit a message {}", this.getLocalEndpointId(), JsonUtils.objectToString(message));
        var leaderId = this.endpoints.getLeaderEndpointId();
        if (UuidTools.equals(message.sourceId, leaderId)) {
            logger.debug("{} submitted message is directly routed to the leader", this.endpoints.getLeaderEndpointId());
            // we can submit here.
            this.raccoon.submit(message);
            return;
        }
        var submission = new SubmitRequest(UUID.randomUUID(), leaderId, message);
        var request = this.gridOpSerDe.serializeSubmitRequest(submission);
        this.send(request);
    }

    /**
     * Make the grid to be sync. if no other endpoint is added then
     * this just returns. if there are available endpoints then this method request a storage sync
     */
    public boolean sync(int timeoutInMs) throws ExecutionException, InterruptedException, TimeoutException {
        this.requiredNotToBeClosed();
        if (UuidTools.equals(this.endpoints.getLocalEndpointId(), this.endpoints().getLeaderEndpointId())) {
            return true;
        }
        logger.info("Sync request started on {}", this.endpoints.getLocalEndpointId());
        var requestId = UUID.randomUUID();
        var message = this.gridOpSerDe.serializeStorageSyncRequest(new StorageSyncRequest(
                requestId,
                this.endpoints.getLocalEndpointId(),
                this.endpoints.getLeaderEndpointId()
        ));
        var promise = new CompletableFuture<StorageSyncResponse>();
        this.pendingStorageSyncRequests.put(requestId, promise);
        this.send(message);
        StorageSyncResponse storageSyncResponse;
        if (0 < timeoutInMs) {
            storageSyncResponse = promise.get(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            storageSyncResponse = promise.get();
        }

        if (storageSyncResponse.commitIndex() - storageSyncResponse.numberOfLogs() <= this.raccoon.getCommitIndex()) {
            // the storage grid is in sync (theoretically)
            logger.debug("Grid {} does not require sync, because the commitIndex is {}, the leader commitIndex is {} and the number of logs the leader has {}, should be sufficient",
                    this.endpoints.getLocalEndpointId(),
                    this.raccoon.getCommitIndex(),
                    storageSyncResponse.commitIndex(),
                    storageSyncResponse.numberOfLogs()
            );
            return true;
        }
        return this.executeSync(storageSyncResponse.commitIndex(), false).get();
    }

    private CompletableFuture<Boolean> executeSync(int newCommitIndex, boolean setCommitIndexEvenIfSomethingFailed) throws TimeoutException, ExecutionException, InterruptedException {
        var result = new CompletableFuture<Boolean>();
        if (!this.pendingSync.compareAndSet(null, result)) {
            return this.pendingSync.get();
        }
        var storageSyncResults = new ConcurrentHashMap<String, CompletableFuture<StorageSyncResult>>();
        for (var storageInGrid : this.storages.values()) {
            var storageSyncResultPromise = this.executors.getSyncOperationExecutor()
                    .submit(() -> storageInGrid.executeSync());
            var completableFuture = Utils.makeCompletableFuture(storageSyncResultPromise);
            storageSyncResults.put(storageInGrid.getIdentifier(), completableFuture);
        }
        try {
            CompletableFuture.allOf(storageSyncResults.values().toArray(new CompletableFuture[storageSyncResults.size()])).thenRun(() -> {
                var success = true;
                for (var storageSyncResultEntry : storageSyncResults.entrySet()) {
                    var storageId = storageSyncResultEntry.getKey();
                    StorageSyncResult storageSyncResult = null;
                    try {
                        storageSyncResult = storageSyncResultEntry.getValue().get();
                    } catch (Exception e) {
                        logger.warn("Error occurred while getting results from storage sync", e);
                        success = false;
                        continue;
                    }
                    success &= storageSyncResult.success();
                    if (storageSyncResult.errors() != null && 0 < storageSyncResult.errors().size()) {
                        logger.warn("Error occurred syncing storage {}. Errors: {}", storageId, storageSyncResult.errors().stream().collect(Collectors.joining(", ")));
                    }
                }
                if (success || setCommitIndexEvenIfSomethingFailed) {
                    if (0 <= newCommitIndex) {
                        this.raccoon.setCommitIndex(newCommitIndex);
                    }
                }
                result.complete(success);
                pendingSync.set(null);
            });
        } catch (Exception ex) {
            logger.error("Error occurred while executing sync", ex);
            pendingSync.set(null);
        }
        return result;
    }

    /**
     * Observable event fired when errors occur in the grid
     * @return Observable event
     */
    public Observable<HamokError> errors() {
        return this.errors;
    }

    String getContext() {
        return this.context;
    }

    @Override
    public void close() {
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
        this.state = State.CLOSED;
    }

    public interface Endpoints {
        UUID getLocalEndpointId();
        Set<UUID> getRemoteEndpointIds();
        UUID getLeaderEndpointId();
    }

    public interface RaftInfo {
        int getCommitIndex();
    }

    public record Events(
            /**
             * Observable events fired when the leader id is changed
             * @return Observable interface
             */
            Observable<Optional<UUID>> changedLeaderId,
            /**
             * Observable event fired when a remote endpoint is joined to the grid
             * @return Observable interface
             */
            Observable<UUID> joinedRemoteEndpoints,
            /**
             * Observable event fired when a remote endpoint is left from the grid
             * @return Observable event
             */
            Observable<UUID> detachedRemoteEndpoints

    ) {

    }

    private void requiredNotToBeClosed() {
        if (State.CLOSED.equals(this.state)) {
            throw new IllegalStateException("StorageGrid must not be to perform the requested operation");
        }
    }
}
