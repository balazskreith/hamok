package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.raccoons.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Set;

class LeaderState extends AbstractState {

    private static final Logger logger = LoggerFactory.getLogger(LeaderState.class);
    private volatile long lastRemoteEndpointChecked = -1;
    private final int currentTerm;

    LeaderState(
            Raccoon base
    ) {
        super(base);
        this.currentTerm = this.syncedProperties().currentTerm.incrementAndGet();
    }

    @Override
    void start() {
        logger.warn("CHECKPOINT 1");
        this.base.setActualLeaderId(this.config().id());
        logger.warn("CHECKPOINT 2");
        this.updateFollowers();
        logger.warn("CHECKPOINT 3");
    }

    @Override
    public RaftState getState() {
        return RaftState.LEADER;
    }

    @Override
    public Integer submit(byte[] entry) {
        return this.logs().submit(this.currentTerm, entry);
    }

    @Override
    void receiveVoteRequested(RaftVoteRequest request) {

    }

    @Override
    void receiveVoteResponse(RaftVoteResponse response) {

    }

    @Override
    void receiveRaftAppendEntriesRequest(RaftAppendEntriesRequest request) {

    }

    @Override
    void receiveRaftAppendEntriesResponse(RaftAppendEntriesResponse response) {
        if (response.term() < this.currentTerm) {
            // this response comes from a previous term, I should not apply it in any way
            return;
        }
        if (this.currentTerm < response.term()) {
            // I am not the leader anymore, so it is best to go back to a follower state
            this.base.setActualLeaderId(null);
            this.follow();
            return;
        }
        // now we are talking in my term...
        if (!response.success()) {
            return;
        }
        var remotePeers = this.remotePeers();
        remotePeers.touch(response.sourcePeerId());

        var logs = this.logs();
        var props = this.syncedProperties();
        var peerNextIndex = response.peerNextIndex();
        var remotePeerIds = remotePeers.getActiveRemotePeerIds();
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
            for (var peerId : remotePeerIds) {
                var matchIndex = props.matchIndex.getOrDefault(peerId, -1);
                if (logEntry.index() <= matchIndex) {
                    ++matchCount;
                }
            }
            if (remotePeerIds.size() + 1 < matchCount * 2) {
                logs.commit();
            }
        }
    }

    @Override
    void receiveHelloNotification(HelloNotification notification) {
        var remotePeerId = notification.sourcePeerId();
        if (remotePeerId == null) {
            logger.warn("Hello notification does not contain a source id");
            return;
        }
        // if we receive a hello notification from any peer we immediately respond with the endpoint state notification.
        // this makes a new peer to receive all remote endpoint from the leader, or if it is inactivated
        // it receives that information from the leader too.
        this.sendEndpointStateNotification(Set.of(remotePeerId));
        var remotePeers = this.remotePeers();
        var hashBefore = remotePeers.hashCode();
        remotePeers.touch(remotePeerId);
        var hashAfter = remotePeers.hashCode();
        if (hashBefore != hashAfter) {
            this.sendEndpointStateNotification(Set.of(remotePeerId));
        }
    }

    @Override
    void receiveEndpointNotification(EndpointStatesNotification notification) {
        logger.warn("{} is a leader and received endpoint state notification from {}. ", notification.destinationEndpointId(), notification.sourceEndpointId());
    }

    @Override
    public void run() {
        this.updateFollowers();
        var config = this.config();
        if (config.autoDiscovery()) {
            this.checkRemotePeers();
        }
    }

    private void checkRemotePeers() {
        var config = this.config();
        var remotePeers = this.remotePeers();
        var now = Instant.now().toEpochMilli();
        if (this.lastRemoteEndpointChecked < 0) {
            this.sendEndpointStateNotification(remotePeers.getActiveRemotePeerIds());
            this.lastRemoteEndpointChecked = now;
            return;
        } else if (now - this.lastRemoteEndpointChecked < config.peerMaxIdleTimeInMs()) {
            return;
        }
        boolean endpointStateChanged = false;
        for (var it = remotePeers.iterator(); it.hasNext(); ) {
            var remotePeer = it.next();
            var peerId = remotePeer.id();
            if (!remotePeer.active()) {
                continue;
            }
            if (0 < config.peerMaxIdleTimeInMs() && config.peerMaxIdleTimeInMs() < now - remotePeer.touched()) {
                remotePeers.detach(peerId);
                endpointStateChanged = true;
                continue;
            }
        }
        if (endpointStateChanged) {
            if (0 < remotePeers.getActiveRemotePeerIds().size()) {
                this.sendEndpointStateNotification(remotePeers.getActiveRemotePeerIds());
            } else {
                // This is the endpoint, which become inactive, not the others!
                logger.info("{} Every remote endpoint become inactive, except this endpoint. Reset is performed", this.getLocalPeerId());
                remotePeers.reset();
                this.follow();
            }
        }
    }


    private void updateFollowers() {
        var config = this.config();
        var props = this.syncedProperties();
        var logs = this.logs();
        var remotePeers = this.remotePeers();
        for (var it = remotePeers.iterator(); it.hasNext(); ) {
            var remotePeer = it.next();
            var peerId = remotePeer.id();
            if (!remotePeer.active()) {
                continue;
            }
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
                        this.getLocalPeerId(),
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
//            logger.info("{} sending appendEntriesRequest {}", this.getLocalPeerId(), appendEntries);
            this.sendAppendEntriesRequest(appendEntries);
        }
    }
}
