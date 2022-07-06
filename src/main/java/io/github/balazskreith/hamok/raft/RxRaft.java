package io.github.balazskreith.hamok.raft;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raft.events.Events;
import io.github.balazskreith.hamok.raft.events.RaftTransport;
import io.github.balazskreith.hamok.rxutils.RxAtomicReference;
import io.github.balazskreith.hamok.rxutils.RxTimeLimitedMap;
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
import java.util.*;
import java.util.concurrent.Executor;

public class RxRaft implements Disposable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(RxRaft.class);

    public static Builder builder() {
        return new Builder();
    }

    private RaftLogs logs;
    private RaftConfig config;
    private Scheduler scheduler;
    private volatile boolean started = false;
    private int flipFlopped = 0;

    private final SyncedProperties syncProperties;
    private final Events inboundEvents = new Events();
    private final Events outboundEvents = new Events();
    private final Subject<Integer> requestCommitIndexSync = PublishSubject.create();
    private final Subject<RaftState> changedState = PublishSubject.create();
    private final Subject<Long> stackedState = PublishSubject.create();
    private final RxAtomicReference<UUID> actualLeaderId = new RxAtomicReference<>(null, UuidTools::equals);
    private final CompositeDisposable disposer = new CompositeDisposable();
    private final RaftTransport transport;

    private Actor actualActor;

    private RxRaft() {
        this.syncProperties = new SyncedProperties();
        this.disposer.addAll(
                this.inboundEvents,
                this.outboundEvents
        );
        this.disposer.add(Disposable.fromRunnable(() -> {
            this.stop();
        }));
        this.transport = RaftTransport.createFrom(this.inboundEvents, this.outboundEvents);
    }

    public RaftTransport transport() {
        return this.transport;
    }

    public RxRaft start() {
        if (this.started) {
            logger.warn("Attempted to start Raft twice");
            return this;
        }
        this.started = true;
        this.changeActor(new Follower(this));
        return this;
    }

    public RxRaft stop() {
        if (!this.started) {
            return this;
        }
        if (this.actualActor != null) {
            this.actualActor.stop();
            this.actualActor = null;
        }
        this.started = false;
        return this;
    }

    public RaftState getState() {
        if (this.actualActor == null) {
            return RaftState.NONE;
        }
        return this.actualActor.getState();
    }

    public UUID getLeaderId() {
        return this.actualLeaderId.get();
    }

    public Observable<Optional<UUID>> changedLeaderId() { return this.actualLeaderId; }
    public Observable<RaftState> changedState() { return this.changedState; }

    public Observable<LogEntry> committedEntries() { return this.logs.committedEntries(); }

    public Observable<Integer> commitIndexSyncRequests() { return this.requestCommitIndexSync; }

    public Observable<Long> stackedState() { return this.stackedState; }


    /**
     * Returns the index of the entry submitted to the leader
     * or null if the current state is not the leader
     *
     * @param entry
     * @return
     */
    public Integer submit(byte[] entry) {
        if (this.actualActor == null) {
            return null;
        }
        return this.actualActor.submit(entry);
    }

    public UUID getId() {
        return this.config.id();
    }

    public void addPeerId(UUID... peerIds) {
        if (peerIds == null || peerIds.length < 1) return;
        this.syncProperties.peerIds.addAll(List.of(peerIds));
    }

    public void removePeerId(UUID... peerIds) {
        if (peerIds == null || peerIds.length < 1) return;
        for (int i = 0; i < peerIds.length; ++i) {
            var peerId = peerIds[i];
            this.syncProperties.peerIds.remove(peerId);
            if (this.actualActor == null) {
                // the follower remote peer inactivated a peerId, and we need to intervene if the actual state is not follower
                this.actualActor.removedPeerId(peerId);
                return;
            }
        }

    }

    void setActualLeaderId(UUID actualLeaderId) {
        this.actualLeaderId.set(actualLeaderId);
    }

    RaftConfig config() {
        return this.config;
    }

    SyncedProperties syncedProperties() {
        return this.syncProperties;
    }

    Scheduler scheduler() {
        return this.scheduler;
    }

    RaftLogs logs() {
        return this.logs;
    }

    Events inboundEvents() { return this.inboundEvents; }

    Events outboundEvents() { return this.outboundEvents; }

    void requestCommitIndexSync() {
        if (!this.requestCommitIndexSync.hasObservers()) {
            throw new IllegalStateException("If log entries are disappeared by the server, the application MUST provide a way to synchronize entries");
        }
        this.requestCommitIndexSync.onNext(1);
    }

    void changeActor(AbstractActor nextActor) {
        RaftState changedState = nextActor != null ? nextActor.getState() : RaftState.NONE;
        synchronized (this) {
            RaftState predecessorState = null;
            if (this.actualActor != null) {
                predecessorState = this.actualActor.getState();
                this.actualActor.stop();
            }
            this.actualActor = nextActor;
            this.actualActor.start();
            logger.info("{} changed role from {} to {}", config.id(), predecessorState, this.actualActor.getState().name());
            if (RaftState.FOLLOWER.equals(predecessorState) && RaftState.CANDIDATE.equals(nextActor.getState())) {
                // this is also flip-flop, but we don't count this one
                ++this.flipFlopped;
            } else if (RaftState.CANDIDATE.equals(predecessorState) && RaftState.FOLLOWER.equals(nextActor.getState())) {
                if (5 == this.flipFlopped) {
                    // we are stacked, we should inform the upper layer and probably inactivate
                    this.stackedState.onNext(Instant.now().toEpochMilli());
                }
            } else {
                this.flipFlopped = 0;
            }
        }
        this.changedState.onNext(changedState);
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
        if (this.actualActor == null) {
            logger.warn("Cannot set the commit index if there is no Raft State");
            return;
        }
        if (this.actualActor.getState() != RaftState.FOLLOWER) {
            logger.warn("Only Follower can sync with commit from API");
            return;
        }
        this.logs.reset(commitIndex);
        logger.info("{} Logs are restarted to point to the application defined commit index {}. Every logs are purged", this.getId(), commitIndex);
    }

    public int getCommitIndex() {
        return this.logs.getCommitIndex();
    }


    public static class Builder {
//        private Executors
        private RxRaft result = new RxRaft();
        private Map<Integer, LogEntry> providedMap;
        private int logExpirationTimeInMs = 10000;
        public Builder withConfig(RaftConfig config) {
            this.result.config = config;
            return this;
        }

        public Builder withExecutor(Executor executor) {
            if (executor == null) return this;
            this.result.scheduler = Schedulers.from(executor);
            return this;
        }

        public Builder withScheduler(Scheduler scheduler) {
            if (scheduler == null) return this;
            this.result.scheduler = scheduler;
            return this;
        }

        public Builder withLogsExpirationTimeInMs(Integer expirationTimeInMs) {
            this.logExpirationTimeInMs = expirationTimeInMs;
            return this;
        }

        public Builder withLogsMap(Map<Integer, LogEntry> map) {
            if (map == null) return this;
            this.logExpirationTimeInMs = 0;
            this.providedMap = map;
            return this;
        }

        public RxRaft build() {
            try {
                Objects.requireNonNull(this.result.config, "Raft cannot be build without config");
                if (this.result.scheduler == null) {
                    logger.info("No scheduler has been provided, computation scheduler from RxJava will be used");
                    this.result.scheduler = Schedulers.computation();
                }
                if (this.providedMap == null) {
                    logger.info("Raft uses time limited map for logs. Expiration time is {} ms", this.logExpirationTimeInMs);
                    var map = new RxTimeLimitedMap<Integer, LogEntry>(this.logExpirationTimeInMs);
                    this.result.logs = new RaftLogs(map);

                    // this is pivotal here to feed back the map on a different thread from its emission.
                    // otherwise we can cause a deadlock
                    var disposable = map.expiredEntry().observeOn(Schedulers.io()).subscribe(entry -> {
                        this.result.logs.expire(entry.getKey());
                    });
                    this.result.logs = new RaftLogs(map);
                    this.result.disposer.add(disposable);
                } else {
                    if (0 < this.logExpirationTimeInMs) {
                        logger.warn("Building with custom map makes the provided building attribute logExpiration time ineffective. If expiration of entry is needed it need to be inside the provided map.");
                    }
                    this.result.logs = new RaftLogs(this.providedMap);
                }
                this.result.disposer.add(this.result.logs.committedEntries().subscribe(logEntry -> {
                    this.result.syncProperties.lastApplied.set(logEntry.index());
                }));
                return this.result;
            } catch (Exception ex) {
                this.result.dispose();
                throw ex;
            }

        }
    }
}
