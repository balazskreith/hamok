package io.github.balazskreith.vstorage.raft;

import io.github.balazskreith.vstorage.raft.events.RaftAppendEntriesRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

class Leader extends AbstractActor {

    private static final Logger logger = LoggerFactory.getLogger(Leader.class);


    private final int currentTerm;

    Leader(
            RxRaft raft
    ) {
        super(raft);
        this.currentTerm = raft.syncedProperties().currentTerm.get();
        this.raft.setActualLeaderId(this.raft.getId());
        this.onAppendEntriesResponse(response -> {
//            logger.info("{} rcv BEGIN appendEntriesResponse {}", this.getId(), response);
            if (response.term() < this.currentTerm) {
                // this response comes from a previous term, I should not apply it in any way
                return;
            }
            if (this.currentTerm < response.term()) {
                // I am not the leader anymore, so it is best to go back to a follower state
                this.raft.setActualLeaderId(null);
                this.raft.changeActor(new Follower(this.raft));
                return;
            }
            // now we are talking in my term...
            if (!response.success()) {
                return;
            }
            var logs = this.raft.logs();
            var props = this.raft.syncedProperties();
            var peerNextIndex = response.peerNextIndex();
            props.nextIndex.put(response.sourcePeerId(), peerNextIndex);
            props.matchIndex.put(response.sourcePeerId(), peerNextIndex - 1);
            for (var it = logs.safeIterator(); it.hasNext(); ) {
                var logEntry = it.next();
                if (peerNextIndex <= logEntry.index()) {
                    break;
                }
                // is this good here? so we will never commit things not created by our term?
                if (logEntry.term() != this.currentTerm) {
                    continue;
                }
                var matchCount = 1;
                for (var peerId : props.peerIds) {
                    var matchIndex = props.matchIndex.getOrDefault(peerId, -1);
                    if (logEntry.index() <= matchIndex) {
                        ++matchCount;
                    }
                }
                if (props.peerIds.size() + 1 < matchCount * 2) {
                    logs.commit();
                }
            }
        });
    }

    @Override
    public RaftState getState() {
        return RaftState.LEADER;
    }

    @Override
    public void start() {
        var config = this.raft.config();
        var disposable = this.raft.scheduler().createWorker()
                .schedulePeriodically(this::update, config.heartbeatInMs(),config.heartbeatInMs(), TimeUnit.MILLISECONDS);
        this.addDisposable(disposable);
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void removedPeerId(UUID peerId) {

    }

    @Override
    public Integer submit(byte[] entry) {
        return this.raft.logs().submit(this.currentTerm, entry);
    }

    private void update() {
        if (this.isDisposed()) {
            logger.warn("{} {} state is being tried to be updated", this.getId(), this.getState().name());
            return;
        }
        var config = this.raft.config();
        var props = this.raft.syncedProperties();
        var logs = this.raft.logs();
//        logger.info("{} Peers {}", this.getId(), JsonUtils.objectToString(props.peerIds));
        for (var it = props.peerIds.iterator(); it.hasNext(); ) {
            var peerId = it.next();
            var peerNextIndex = props.nextIndex.getOrDefault(peerId, 0);
            var prevLogIndex = peerNextIndex - 1;
            var prevLogTerm = -1;
            if (0 <= prevLogIndex) {
                var logEntry = logs.get(prevLogIndex);
                if (logEntry != null) {
                    prevLogTerm = logEntry.term();
                }
            }
            var entries = logs.collectEntries(peerNextIndex);
            if (peerNextIndex < logs.getLastApplied()) {
                logger.warn("{} collected {} entries, but peer {} should need {}. The peer should request a commit sync",
                        this.getId(),
                        entries.size(),
                        peerId,
                        logs.getNextIndex() - peerNextIndex
                );
            }
            var appendEntries = new RaftAppendEntriesRequest(
                    peerId,
                    this.currentTerm,
                    config.id(),
                    prevLogIndex,
                    prevLogTerm,
                    entries,
                    logs.getCommitIndex(),
                    logs.getNextIndex()
            );
//            logger.info("{} sending appendEntriesRequest {}", this.getId(), appendEntries);
            this.sendAppendEntriesRequest(appendEntries);
        }
    }
}
