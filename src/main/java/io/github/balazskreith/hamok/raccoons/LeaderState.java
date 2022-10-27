package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.KeyValuePair;
import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class LeaderState extends AbstractState {

    private static final Logger logger = LoggerFactory.getLogger(LeaderState.class);
    private volatile long lastRemoteEndpointChecked = -1;
    /**
     * leaders should track the sent index per peers. the reason behinf that is if
     * the response to the append request chunks arrives slower than the updateFollower is called,
     * then the same chunks is sent to follower making it slower to respond, making this leader sending
     * the same append request with the same entries more, making the follower even slower than before,
     * and the system explode. This tracking preventing to sending the same chunk of request twice
     * until the follower does not respond normally.
     */
    private final Map<UUID, KeyValuePair<UUID, Long>> sentRequests = new ConcurrentHashMap<>();
    private final int currentTerm;
    private final Set<UUID> unsyncedRemotePeers = new HashSet<>();
    private final AtomicReference<Set<UUID>> activeRemotePeerIds;

    LeaderState(
            Raccoon base
    ) {
        super(base);
        this.currentTerm = this.syncedProperties().currentTerm.incrementAndGet();
        this.activeRemotePeerIds = new AtomicReference<>(this.remotePeers().getRemotePeerIds());
    }

    @Override
    void start() {
        this.base.setActualLeaderId(this.config().id());
        this.updateFollowers();

    }

    @Override
    public RaftState getState() {
        return RaftState.LEADER;
    }

    @Override
    public boolean submit(Models.Message entry) {
        logger.trace("{} submitted entry started for message {}", this.getLocalPeerId(), entry);
        this.logs().submit(this.currentTerm, entry);
        return true;
    }

    @Override
    void receiveVoteRequested(RaftVoteRequest request) {
        // until this node is alive and have higher or equal term in append requests then anyone else,
        // it should vote false to any candidate request
        var response = request.createResponse(false);
        this.sendVoteResponse(response);
    }

    @Override
    void receiveVoteResponse(RaftVoteResponse response) {
        var remotePeers = this.remotePeers();
        if (response.sourcePeerId() != null && remotePeers.get(response.sourcePeerId()) != null) {
            // let's keep up to date the last touches
            remotePeers.touch(response.sourcePeerId());
        }
    }

    @Override
    void receiveRaftAppendEntriesRequestChunk(RaftAppendEntriesRequestChunk request) {
        if (request == null || request.term() < this.currentTerm) {
            return;
        }
        if (request.leaderId() == null) {
            logger.warn("Append Request Chunk is received without leaderId {}", request);
            return;

        }
        if (UuidTools.equals(request.leaderId(), this.getLocalPeerId())) {
            // loopback message?
            return;
        }
        if (this.currentTerm < request.term()) {
            logger.warn("Current term of the leader is {}, and received Request Chunk {} have higher term.");
            this.follow();
            return;
        }
        // terms are equal
        logger.warn("Append Request Chunk is received from another leader in the same term. Selecting one leader in this case who has higher id. {}", request);
        if (this.getLocalPeerId().getMostSignificantBits() < request.leaderId().getMostSignificantBits()) {
            // only one can remain!
            this.follow();
        }
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
        logger.trace("Received RaftAppendEntriesResponse {}", response);
        var remotePeers = this.remotePeers();
        if (UuidTools.notEquals(this.getLocalPeerId(), response.sourcePeerId())) {
            remotePeers.touch(response.sourcePeerId());
        }

        // processed means the remote peer processed all the chunks for the request
        if (!response.processed()) {
            return;
        }

        // success means that the other end successfully accepted the request
        if (!response.success()) {
            // having unsuccessful response, but proceeded all of the chunks
            // means we should or can send a request again if it was a complex one.
            this.sentRequests.remove(response.sourcePeerId());
            return;
        }
        var sourcePeerId = response.sourcePeerId();
        var sentRequest = this.sentRequests.remove(sourcePeerId);
        if (sentRequest == null) {
            // most likely a response to a keep alive or expired request
            return;
        }

        var logs = this.logs();
        var props = this.syncedProperties();
        var peerNextIndex = response.peerNextIndex();
        var remotePeerIds = remotePeers.getRemotePeerIds();

        props.nextIndex.put(sourcePeerId, peerNextIndex);
        props.matchIndex.put(sourcePeerId, peerNextIndex - 1);
        int maxCommitIndex = -1;
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
            logger.trace("logIndex: {}, matchCount: {}, remotePeerIds: {} commit: {}", logEntry.index(), matchCount, remotePeerIds.size(), remotePeerIds.size() + 1 < matchCount * 2);
            if (remotePeerIds.size() + 1 < matchCount * 2) {
                maxCommitIndex = Math.max(maxCommitIndex, logEntry.index());
            }
        }
        if (0 <= maxCommitIndex) {
            logger.debug("Committing index until {} at leader state", maxCommitIndex);
            var committedLogEntries = logs.commitUntil(maxCommitIndex);
            committedLogEntries.forEach(this::commitLogEntry);
        }
    }

    @Override
    void receiveHelloNotification(HelloNotification notification) {
        var remotePeerId = notification.sourcePeerId();
        if (remotePeerId == null) {
            logger.warn("Hello notification does not contain a source id");
            return;
        } else if (UuidTools.equals(remotePeerId, this.getLeaderId()) || UuidTools.equals(remotePeerId, this.getLocalPeerId())) {
            // why I got a hello from myself?
            logger.warn("Got hello messages from the node itself");
            return;
        }
        // if we receive a hello notification from any peer we immediately respond with the endpoint state notification.
        var remotePeers = this.remotePeers();
        var hashBefore = remotePeers.hashCode();
        remotePeers.touch(remotePeerId);
        var hashAfter = remotePeers.hashCode();
        if (hashBefore == hashAfter) {
            // if nothing has changed we just acknoledge the hello
            this.sendEndpointStateNotification(Set.of(remotePeerId), this.activeRemotePeerIds.get());
            return;
        }
        this.updateActiveRemotePeerIds();

        // otherwise we send the new situation to all peers
        // and request the new peer to perform a sync
        this.sendEndpointStateNotification(remotePeers.getRemotePeerIds(), this.activeRemotePeerIds.get());
    }

    @Override
    void receiveEndpointNotification(EndpointStatesNotification notification) {
        logger.warn("{} is a leader and received endpoint state notification from {}. ", notification.destinationEndpointId(), notification.sourceEndpointId());
    }

    @Override
    public void run() {
        this.updateFollowers();
        if (this.updateActiveRemotePeerIds()) {
            this.sendEndpointStateNotification(remotePeers().getRemotePeerIds(), this.activeRemotePeerIds.get());
        }
        if (this.remotePeers().size() < 1) {
            logger.warn("Leader endpoint become a follower because no remote endpoint is available");
            this.follow();
        }
    }

    /**
     *
     * @return true if anything changed in the active remote peer ids, false otherwise
     */
    private boolean updateActiveRemotePeerIds() {
        var config = this.config();
        var now = Instant.now().toEpochMilli();
        if (this.lastRemoteEndpointChecked < 0) {
            this.lastRemoteEndpointChecked = now;
            return false;
        } else if (now - this.lastRemoteEndpointChecked < config.peerMaxIdleTimeInMs()) {
            return false;
        }
        this.lastRemoteEndpointChecked = now;

        var currentActiveRemotePeerIds = this.activeRemotePeerIds.get();
        var remotePeers = this.remotePeers();
        if (config.peerMaxIdleTimeInMs() < 1) {
            this.activeRemotePeerIds.set(remotePeers.getRemotePeerIds());
            return !SetUtils.isContentsEqual(currentActiveRemotePeerIds, this.activeRemotePeerIds.get());
        }

        var newActiveRemotePeerIds = new HashSet<UUID>();
        for (var it = remotePeers.iterator(); it.hasNext(); ) {
            var remotePeer = it.next();
            if (config.peerMaxIdleTimeInMs() < now - remotePeer.touched()) {
                continue;
            }
            newActiveRemotePeerIds.add(remotePeer.id());
        }
        this.activeRemotePeerIds.set(newActiveRemotePeerIds);
        return !SetUtils.isContentsEqual(currentActiveRemotePeerIds, newActiveRemotePeerIds);
    }

    private void updateFollowers() {
        var config = this.config();
        var props = this.syncedProperties();
        var logs = this.logs();
        var remotePeers = this.remotePeers();
        var now = Instant.now().toEpochMilli();
        for (var it = remotePeers.iterator(); it.hasNext(); ) {
            var remotePeer = it.next();
            var peerId = remotePeer.id();
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
                if (this.unsyncedRemotePeers.add(peerId)) {
                    logger.warn("{} collected {} entries, but peer {} should need {}. The peer should request a commit sync",
                            this.getLocalPeerId(),
                            entries.size(),
                            peerId,
                            logs.getNextIndex() - peerNextIndex
                    );
                }
            } else if (this.unsyncedRemotePeers.isEmpty() == false) {
                this.unsyncedRemotePeers.remove(peerId);
            }
            var sentRequest = this.sentRequests.get(peerId);
            if (sentRequest != null) {
                // we kill the sent request if it is older than the threshold
                if (sentRequest.getValue() < now - 30000) {
                    this.sentRequests.remove(peerId);
                    sentRequest = null;
                }
            }
            UUID requestId = UUID.randomUUID();
            // we should only sent an entryfull request if the remote peer does not have one, and we have something to add
            if (sentRequest == null && entries != null && 0 < entries.size()) {
                for (int sequence = 0; sequence < entries.size(); ++sequence) {
                    var entry = entries.get(sequence);
                    var appendEntries = new RaftAppendEntriesRequestChunk(
                            peerId,
                            this.currentTerm,
                            config.id(),
                            prevLogIndex,
                            prevLogTerm,
                            entry,
                            logs.getCommitIndex(),
                            logs.getNextIndex(),
                            sequence,
                            sequence == entries.size() - 1,
                            requestId
                    );
//                    logger.info("Sending {}", appendEntries);
                    this.sendAppendEntriesRequestChunk(appendEntries);
                }
                sentRequest = KeyValuePair.of(requestId, now);
                this.sentRequests.put(peerId, sentRequest);
            } else { // no entries
                var appendEntries = new RaftAppendEntriesRequestChunk(
                        peerId,
                        this.currentTerm,
                        config.id(),
                        prevLogIndex,
                        prevLogTerm,
                        null,
                        logs.getCommitIndex(),
                        logs.getNextIndex(),
                        0,
                        true,
                        requestId
                );
                this.sendAppendEntriesRequestChunk(appendEntries);
            }
        }
    }
}
