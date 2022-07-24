package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.common.UuidTools;
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
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class Raccoon implements Disposable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Raccoon.class);

    public static RaccoonBuilder builder() {
        return new RaccoonBuilder();
    }

    final RaftLogs logs;
    final RaccoonConfig config;
    final Scheduler scheduler;

    final SyncedProperties syncProperties = new SyncedProperties();
    final RemotePeers remotePeers = new RemotePeers();

    final Events inboundEvents = new Events();
    final Events outboundEvents = new Events();

    private final Subject<Integer> requestCommitIndexSync = PublishSubject.create();
    private final Subject<Long> inactivatedLocalPeer = PublishSubject.create();
    private final Subject<RaftState> changedState = PublishSubject.create();
    private final RxAtomicReference<UUID> actualLeaderId = new RxAtomicReference<>(null, UuidTools::equals);
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
        this.disposer.add(this.inboundEvents.appendEntriesRequest().subscribe(request -> {
            var state = this.actual.get();
            if (state == null) {
                logger.warn("No state is active, but got a request");
                return;
            }
            state.receiveRaftAppendEntriesRequest(request);
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

    public Observable<LogEntry> committedEntries() { return this.logs.committedEntries().observeOn(Schedulers.computation()); }

    public Observable<Integer> commitIndexSyncRequests() { return this.requestCommitIndexSync.observeOn(Schedulers.computation()); }

    public Observable<UUID> joinedRemotePeerId() { return this.remotePeers.joinedRemoteEndpointIds().observeOn(Schedulers.computation()); }

    public Observable<UUID> detachedRemotePeerId() { return this.remotePeers.detachedRemoteEndpointIds().observeOn(Schedulers.computation()); }

    public Observable<Long> inactivatedLocalPeer() {return this.inactivatedLocalPeer.observeOn(Schedulers.computation()); }

    /**
     * Returns the index of the entry submitted to the leader
     * or null if the current state is not the leader
     *
     * @param entry
     * @return
     */
    public boolean submit(byte[] entry) {
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

    public Set<UUID> getRemoteEndpointIds() {
        return this.remotePeers.getActiveRemotePeerIds();
    }

    public void addRemotePeerId(UUID peerId) {
        this.remotePeers.join(peerId);
    }

    public void removeRemotePeerId(UUID peerId) {
        this.remotePeers.detach(peerId);
    }

    void signalInactivatedLocalPeer() {
        this.inactivatedLocalPeer.onNext(Instant.now().toEpochMilli());
    }

    void setActualLeaderId(UUID actualLeaderId) {
        this.actualLeaderId.set(actualLeaderId);
    }

    void requestCommitIndexSync() {
        if (!this.requestCommitIndexSync.hasObservers()) {
            throw new IllegalStateException("If log entries are disappeared by the server, the application MUST provide a way to synchronize entries");
        }
        this.requestCommitIndexSync.onNext(1);
    }

    void changeState(AbstractState successor) {
        if (successor == null) {
            if (!this.actual.compareAndSet(null, null)) {
                this.actual.set(null);
                this.changedState.onNext(RaftState.NONE);
                logger.info("{} Changed state to null", this.getId());
            }
            return;
        }
        var state = this.actual.get();
        this.actual.set(successor);
        if (state == null) {
            this.changedState.onNext(successor.getState());
            logger.info("{} Changed state from null to {}", this.getId(), successor.getState());
        } else if (!successor.getState().equals(state.getState())) {
            this.changedState.onNext(successor.getState());
            logger.info("{} Changed state from {} to {}", this.getId(), state.getState(), successor.getState());
        }
        successor.start();
    }
}
