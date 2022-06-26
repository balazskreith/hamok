package io.github.balazskreith.vstorage.raft;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

class Follower extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(Follower.class);
    private long lastRefreshed = 0;
    private Disposable timer;
    private volatile boolean syncRequested = false;
    private final Scheduler.Worker worker;

    Follower(RxRaft raft) {
        super(raft);
        this.worker = this.raft.scheduler().createWorker();
        var props = raft.syncedProperties();
        props.votedFor.set(null);
        this.onAppendEntriesRequest(request -> {
//            logger.info("{} received message from {}", this.getId(), request.leaderId());
            var currentTerm = props.currentTerm.get();
            if (request.term() < currentTerm) {
                logger.warn("{} Append entries request appeared from a previous term. currentTerm: {}, received entries request term: {}", this.getId(), currentTerm, request.term());
                var response = request.createResponse(false, -1);
                this.sendAppendEntriesResponse(response);
                return;
            }
            if (currentTerm < request.term()) {
                logger.info("{} Term for follower has been changed from {} to {}", this.getId(), currentTerm, request.term());
                currentTerm = request.term();
                props.currentTerm.set(currentTerm);
                props.votedFor.set(null);
            }
            if (request.entries() == null) {
                logger.warn("{} Entries cannot be null", this.getId());
                var response = request.createResponse(false, -1);
                this.sendAppendEntriesResponse(response);
                return;
            }
            // let's restart the timer
            this.restart();
            // and let's set who is the boss now
            this.raft.setActualLeaderId(request.leaderId());

            var logs = this.raft.logs();

            // let's check if we are covered with the entries or not
            if (logs.getNextIndex() < request.leaderNextIndex() - request.entries().size()) {
                if (!this.syncRequested) {
                    logger.info("{} request a commit sync as the owned next index is {} and the leader next index is {}, and the provided number of entries ({}) insufficient to close the gap.",
                            this.getId(),
                            logs.getNextIndex(),
                            request.leaderNextIndex(),
                            request.entries().size()
                    );
                    this.raft.requestCommitIndexSync();
                    this.syncRequested = true;
                }
                // until we cannot close the gap we cannot mae a successful response
                var response = request.createResponse(false, -1);
                this.sendAppendEntriesResponse(response);
                return;
            }
            if (this.syncRequested) {
                logger.info("{} seems commit sync is done, the owned next index is {}, the leader next index is {} and the provided number of entries ({}) seems sufficiently close the gap",
                        this.getId(),
                        logs.getNextIndex(),
                        request.leaderNextIndex(),
                        request.entries().size()
                );
            }
            this.syncRequested = false;
            // if we arrived in this point we know that the sync is possible.
            var entryLength = request.entries().size();
            var localNextIndex = logs.getNextIndex();
            var success = true;
            for (int i = 0; i < entryLength; ++i) {
                var logIndex = request.leaderNextIndex() - entryLength + i;
                var entry = request.entries().get(i);
                if (logIndex < localNextIndex) {
                    var oldLogEntry = logs.compareAndOverride(logIndex, currentTerm, entry);
                    if (oldLogEntry != null && currentTerm < oldLogEntry.term()) {
                        logger.warn("We overrode an entry coming from a higher term we currently had. (currentTerm: {}, old log entry term: {}). This can cause a potential inconsistency if other peer has not override it as well", currentTerm, oldLogEntry.term());
                    }
                } else if (!logs.compareAndAdd(logIndex, currentTerm, entry)) {
                    logger.warn("Log for index {} not added, though it supposed to", logIndex);
                    success = false;
                }
            }
            for (int peerCommitIndex = logs.getCommitIndex(); peerCommitIndex < request.leaderCommit(); ) {
                logs.commit();
                peerCommitIndex = logs.getCommitIndex();
            }
            var response = request.createResponse(success, logs.getNextIndex());
//            logger.info("{} sending {}", this.getId(), response);
            this.sendAppendEntriesResponse(response);
        }).onVoteRequested(request -> {
            logger.info("{} received a vote request {}, votedFor: {}", this.getId(), request, props.votedFor.get());
            if (this.raft == null) {
                logger.warn("{} Received a message meanwhile state is not active. ", this.getId());
                return;
            }
            if (request.term() <= props.currentTerm.get()) {
                // someone requested a vote from a previous or equal term.
                this.sendVoteResponse(request.createResponse(false));
                return;
            }
            var syncedProperties = this.raft.syncedProperties();
            var voteGranted = syncedProperties.votedFor.compareAndSet(null, request.candidateId());
            if (!voteGranted) {
                // maybe we already voted for the candidate itself?
                voteGranted = syncedProperties.votedFor.get() == request.candidateId();
            }
            var response = request.createResponse(voteGranted);
            logger.info("{} send a vote response {}.", this.getId(), response);
            this.sendVoteResponse(response);
            if (voteGranted) {
                // let's restart the timer if we voted for someone.
                this.restart();
            }

            // process entries
        }).addDisposable(Disposable.fromRunnable(() -> {
            synchronized (this) {
                if (this.timer != null) {
                    logger.info("{} dispose timer", this.getId());
                    if (!this.timer.isDisposed()) {
                        this.timer.dispose();
                    }
                }
                this.timer = null;
            }
        }));
    }

    @Override
    public Integer submit(byte[] entry) {
        return null;
    }

    private void restart() {
        synchronized (this) {
            if (this.isDisposed()) {
                logger.warn("{} {} state is being tried to be restarted", this.getId(), this.getState().name());
                return;
            }
            if (this.timer != null) {
                if (!this.timer.isDisposed()) {
                    this.timer.dispose();
                }
                this.timer = null;
            }
//            logger.info("{} Restarted", this.getId());
            this.lastRefreshed = Instant.now().toEpochMilli();

            var config = this.raft.config();
            var delayInMs = config.electionMinTimeoutInMs() + new Random().nextInt(config.electionMaxRandomOffsetInMs());
            this.timer = this.worker.schedule(() -> {
                // no leader heartbeat received, we request a new election
                logger.info("{} starts a new election, elapsed time in ms {}", this.getId(), Instant.now().toEpochMilli() - this.lastRefreshed);
                var actor = new Candidate(this.raft);
                this.timer = null;
                this.raft.changeActor(actor);
            }, delayInMs, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public RaftState getState() {
        return RaftState.FOLLOWER;
    }

    @Override
    public void start() {
        this.restart();
    }


    @Override
    public void removedPeerId(UUID peerId) {

    }
}
