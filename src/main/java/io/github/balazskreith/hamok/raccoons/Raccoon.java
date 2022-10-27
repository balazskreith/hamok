package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.events.EndpointStatesNotification;
import io.github.balazskreith.hamok.raccoons.events.Events;
import io.github.balazskreith.hamok.raccoons.events.InboundEvents;
import io.github.balazskreith.hamok.raccoons.events.OutboundEvents;
import io.github.balazskreith.hamok.rxutils.RxAtomicReference;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Raccoon implements Disposable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Raccoon.class);


    public static RaccoonBuilder builder() {
        return new RaccoonBuilder();
    }

    final RaftLogs logs;
    final RaccoonConfig config;

    final SyncedProperties syncProperties = new SyncedProperties();
    final RemotePeers remotePeers = new RemotePeers();

    final Events inboundEvents = new Events();
    final Events outboundEvents = new Events();

    public Subject<LogEntry> committedEntries = PublishSubject.create();
    private final Subject<CompletableFuture<Boolean>> requestStorageSync = PublishSubject.create();
    private final Subject<RaftState> changedState = PublishSubject.create();
    private final Subject<UUID> inactiveRemotePeerId = PublishSubject.create();
    private final RxAtomicReference<UUID> actualLeaderId = new RxAtomicReference<>(null, UuidTools::equals);
    private final Scheduler scheduler;
//    private final RaccoonExecutors
    private final CompositeDisposable disposer;

    private AtomicReference<Disposable> timer = new AtomicReference<>(null);
    private AtomicReference<AbstractState> actual = new AtomicReference<>(null);

    Raccoon(
            RaftLogs logs,
            RaccoonConfig config,
            Scheduler scheduler,
            CompositeDisposable disposer
    ) {
        this.logs = logs;
        this.config = config;
        this.scheduler = scheduler;
        this.disposer = disposer;

        this.disposer.addAll(
                this.inboundEvents,
                this.outboundEvents
        );
        this.disposer.add(this.inboundEvents.appendEntriesRequestChunk().subscribe(requestChunk -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
//            var started = Instant.now().getEpochSecond();
            state.receiveRaftAppendEntriesRequestChunk(requestChunk);
//            logger.info("Received {}", requestChunk);
//            logger.info("receiveRaftAppendEntriesRequestChunk took {} sec", Instant.now().getEpochSecond() - started);
        }));
        this.disposer.add(this.inboundEvents.appendEntriesResponse().subscribe(response -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
            state.receiveRaftAppendEntriesResponse(response);
        }));
        this.disposer.add(this.inboundEvents.voteRequests().subscribe(request -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
            state.receiveVoteRequested(request);
        }));
        this.disposer.add(this.inboundEvents.voteResponse().subscribe(response -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
            state.receiveVoteResponse(response);
        }));
        this.disposer.add(this.inboundEvents.helloNotifications().subscribe(notification -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
            state.receiveHelloNotification(notification);
        }));
        this.disposer.add(this.inboundEvents.endpointStateNotifications().subscribe(notification -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
            state.receiveEndpointNotification(notification);
            this.processEndpointStateNotification(notification);
        }));

        this.disposer.add(this.outboundEvents.endpointStateNotifications().subscribe(endpointStatesNotification -> {
            this.processEndpointStateNotification(endpointStatesNotification);
        }));

        this.disposer.add(Disposable.fromRunnable(() -> {
            this.stop();
        }));
    }

    public void start() {
        if (this.timer.get() != null) {
            logger.warn("Attempted to start twice");
            return;
        }
        Disposable timer = this.scheduler.createWorker().schedulePeriodically(() -> {
            var state = this.actual.get();
//            logger.info("Running raccoon with thread id {}", Thread.currentThread().getId());
            if (state == null) {
                logger.warn("{} No active state to execute", this.getId());
                return;
            }
            state.run();
        }, this.config.heartbeatInMs(), this.config.heartbeatInMs(), TimeUnit.MILLISECONDS);
        if (!this.timer.compareAndSet(null, timer)) {
            timer.dispose();
            return;
        }
        this.changeState(new FollowerState(this));
        logger.info("{} Started", this.config.id());
    }

    public void stop() {
        var timer = this.timer.getAndSet(null);
        if (timer == null) {
            logger.warn("Attempted to stopped twice");
            return;
        }
        timer.dispose();
        this.actual.set(null);
        this.remotePeers.reset();
        logger.info("{} Stopped", this.config.id());
    }

    public InboundEvents inboundEvents() { return InboundEvents.createFrom(this.inboundEvents); }

    public OutboundEvents outboundEvents() { return OutboundEvents.createFrom(this.outboundEvents); }

    public RaftState getState() {
        var state = this.actual.get();
        if (state == null) {
            return RaftState.NONE;
        }
        return state.getState();
    }

    public UUID getLeaderId() {
        return this.actualLeaderId.get();
    }

    public Observable<Optional<UUID>> changedLeaderId() { return this.actualLeaderId.observeOn(Schedulers.computation()); }

    public Observable<RaftState> changedState() { return this.changedState.observeOn(Schedulers.computation()); }

    public Observable<LogEntry> committedEntries() { return this.committedEntries.observeOn(Schedulers.single()); }

    public Observable<UUID> joinedRemotePeerId() { return this.remotePeers.joinedRemoteEndpointIds().observeOn(Schedulers.computation()); }

    public Observable<UUID> detachedRemotePeerId() { return this.remotePeers.detachedRemoteEndpointIds().observeOn(Schedulers.computation()); }

    public Observable<UUID> inactiveRemotePeerId() { return this.inactiveRemotePeerId; }

    /**
     * Returns the index of the entry submitted to the leader
     * or null if the current state is not the leader
     *
     * @param entry
     * @return
     */
    public boolean submit(Models.Message entry) {
        var state = this.actual.get();
        if (state == null) {
            return false;
        }
        return state.submit(entry);
    }

    public UUID getId() {
        return this.config.id();
    }

    @Override
    public void dispose() {
        if (this.disposer.isDisposed()) {
            return;
        }
        this.disposer.dispose();
    }

    @Override
    public boolean isDisposed() {
        return this.disposer.isDisposed();
    }

    @Override
    public void close() throws IOException {
        if (!this.isDisposed()) {
            this.dispose();
        }
    }

    public void setCommitIndex(int commitIndex) {
        var state = this.actual.get();
        if (state == null) {
            logger.warn("Cannot set the commit index if there is no Raft State");
            return;
        }
        if (state.getState() != RaftState.FOLLOWER) {
            logger.warn("Only Follower can sync with commit from API");
            return;
        }
        this.logs.reset(commitIndex);
        logger.info("{} Logs are restarted to point to the application defined commit index {}. Every logs are purged", this.getId(), commitIndex);
    }

    public int getCommitIndex() {
        return this.logs.getCommitIndex();
    }

    public int getNumberOfCommits() {
        return this.logs.size();
    }

    public int getLastApplied() {
        return this.logs.getLastApplied();
    }

    public Set<UUID> getRemoteEndpointIds() {
        return this.remotePeers.getRemotePeerIds();
    }

    public void addRemotePeerId(UUID peerId) {
        this.remotePeers.join(peerId);
    }

    public void removeRemotePeerId(UUID peerId) {
        this.remotePeers.detach(peerId);
    }

    void setActualLeaderId(UUID actualLeaderId) {
        this.actualLeaderId.set(actualLeaderId);
    }

    CompletableFuture<Boolean> requestStorageSync() {
        if (!this.requestStorageSync.hasObservers()) {
            throw new IllegalStateException("If log entries are disappeared by the server, the application MUST provide a way to synchronize entries");
        }
        var result = new CompletableFuture<Boolean>();
        this.requestStorageSync.onNext(result);
        return result;
    }

    public Observable<CompletableFuture<Boolean>> requestedStorageSync() {
        return this.requestStorageSync;
    }

    void changeState(AbstractState successor) {
        if (successor == null) {
            if (!this.actual.compareAndSet(null, null)) {
                this.actual.set(null);
                this.changedState.onNext(RaftState.NONE);
                logger.debug("{} Changed state to null", this.getId());
            }
            return;
        }
        var state = this.actual.get();
        this.actual.set(successor);
        if (state == null) {
            this.changedState.onNext(successor.getState());
            logger.debug("{} Changed state from null to {}", this.getId(), successor.getState());
        } else if (!successor.getState().equals(state.getState())) {
            this.changedState.onNext(successor.getState());
            logger.debug("{} Changed state from {} to {}", this.getId(), state.getState(), successor.getState());
        }
        successor.start();
    }

    private void processEndpointStateNotification(EndpointStatesNotification notification) {
        var reportedActivePeerIds = notification.activeEndpointIds();
        var remotePeerIds = this.remotePeers.getRemotePeerIds();
        for (var remotePeerId : remotePeerIds) {
            if (!reportedActivePeerIds.contains(remotePeerId)) {
                // inactive remote peer id
                this.inactiveRemotePeerId.onNext(remotePeerId);
            }
        }
    }
}
