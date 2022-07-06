package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.events.*;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class FollowerState extends AbstractState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerState.class);
    private AtomicLong updated = new AtomicLong(Instant.now().toEpochMilli());
    private AtomicLong sentHello = new AtomicLong(0);
    private volatile boolean syncRequested = false;
    private volatile int timedOutElection;
    private int extraWaitingTime = 0;

    FollowerState(Raccoon base) {
        this(base, 0);
    }

    FollowerState(Raccoon base, int timedOutElection) {
        super(base);
        this.timedOutElection = timedOutElection;
        if (0 < timedOutElection) {
            var config = this.config();
            var random = new SecureRandom();
            var extraWaitingTimeInMs = random.nextInt(10 * timedOutElection * config.heartbeatInMs());
            this.extraWaitingTime = extraWaitingTimeInMs;
        }
        var props = this.syncedProperties();
        props.votedFor.set(null);
    }

    @Override
    void start() {
        // nothing we do here
    }

    @Override
    public boolean submit(Message entry) {
        return false;
    }

    @Override
    void receiveVoteRequested(RaftVoteRequest request) {
        var props = this.syncedProperties();
        logger.trace("{} received a vote request {}, votedFor: {}", this.getLocalPeerId(), request, props.votedFor.get());
        if (request.term() <= props.currentTerm.get()) {
            // someone requested a vote from a previous or equal term.
            this.sendVoteResponse(request.createResponse(false));
            return;
        }
        var logs = this.logs();
        if (request.lastLogIndex() < logs.getCommitIndex()) {
            // if the highest index of the candidate is smaller than the commit index of this,
            // then that candidate should not lead this cluster, and wait for another leader who can
            this.sendVoteResponse(request.createResponse(false));
            return;
        }
        var voteGranted = props.votedFor.compareAndSet(null, request.candidateId());
        if (!voteGranted) {
            // maybe we already voted for the candidate itself?
            voteGranted = props.votedFor.get() == request.candidateId();
        }
        var response = request.createResponse(voteGranted);
        logger.info("{} send a vote response {}.", this.getLocalPeerId(), response);
        this.sendVoteResponse(response);
        if (voteGranted) {
            // let's restart the timer if we voted for someone.
            this.updated.set(Instant.now().toEpochMilli());
        }
    }

    @Override
    void receiveVoteResponse(RaftVoteResponse response) {
        logger.warn("{} received vote response in followern state {}", this.getLocalPeerId(), response);
    }

    private Map<UUID, RaftAppendEntriesRequest> pendingRequests = new ConcurrentHashMap<>();



    @Override
    void receiveRaftAppendEntriesRequestChunk(RaftAppendEntriesRequestChunk requestChunk) {
        var props = this.syncedProperties();
        var currentTerm = props.currentTerm.get();
        if (requestChunk.term() < currentTerm) {
            logger.warn("{} Append entries request appeared from a previous term. currentTerm: {}, received entries request term: {}", this.getLocalPeerId(), currentTerm, requestChunk.term());
            var response = requestChunk.createResponse(false, -1, true);
            this.sendAppendEntriesResponse(response);
            return;
        }

        if (currentTerm < requestChunk.term()) {
            logger.info("{} Term for follower has been changed from {} to {}", this.getLocalPeerId(), currentTerm, requestChunk.term());
            currentTerm = requestChunk.term();
            props.currentTerm.set(currentTerm);
            props.votedFor.set(null);
        }
        // let's restart the timer
        this.updated.set(Instant.now().toEpochMilli());
        // and make sure next election we don't add unnecessary offset
        this.timedOutElection = 0;

        // assemble here
        var request = this.pendingRequests.get(requestChunk.requestId());
        if (request == null) {
            request = new RaftAppendEntriesRequest(requestChunk.requestId());
            this.pendingRequests.put(requestChunk.requestId(), request);
        }
        request.add(requestChunk);
//        logger.info("Received {}", request);
        if (request.ready() == false) {
            var response = requestChunk.createResponse(true, -1, false);
            this.sendAppendEntriesResponse(response);
            return;
        }
        this.pendingRequests.remove(requestChunk.requestId());


        if (request.entries() == null) {
            logger.warn("{} Entries cannot be null", this.getLocalPeerId());
            var response = requestChunk.createResponse(false, -1, true);
            this.sendAppendEntriesResponse(response);
            return;
        }

        // set the actual leader
        this.setActualLeaderId(request.leaderId());

        var logs = this.logs();

        // let's check if we are covered with the entries or not
        logger.debug("{} next index: {}, request leader next index: {}, request entries size: {}",
                this.getLocalPeerId(),
                logs.getNextIndex(),
                request.leaderNextIndex(),
                request.entries().size()
        );
        if (logs.getNextIndex() < request.leaderNextIndex() - request.entries().size()) {
            if (!this.syncRequested) {
                logger.warn("{} request a commit sync as the owned next index is {} and the leader next index is {}, and the provided number of entries ({}) insufficient to close the gap.",
                        this.getLocalPeerId(),
                        logs.getNextIndex(),
                        request.leaderNextIndex(),
                        request.entries().size()
                );
                this.requestCommitIndexSync(request.leaderId());
                this.syncRequested = true;
            }
            // until we cannot close the gap we cannot mae a successful response
            var response = requestChunk.createResponse(false, -1, true);
            this.sendAppendEntriesResponse(response);
            return;
        }
        if (this.syncRequested) {
            logger.info("{} commit sync is executed, the owned next index is {}, the leader next index is {} and the provided number of entries ({}) seems sufficiently close the gap",
                    this.getLocalPeerId(),
                    logs.getNextIndex(),
                    request.leaderNextIndex(),
                    request.entries().size()
            );
        }
        this.syncRequested = false;
//        logger.info("Received {}", requestChunk);
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
            logger.trace("{} Committing index {} at follower state", this.getLocalPeerId(), peerCommitIndex);
            logs.commit();
            peerCommitIndex = logs.getCommitIndex();
        }
        var response = requestChunk.createResponse(success, logs.getNextIndex(), true);
//            logger.info("{} sending {}", this.getId(), response);
        this.sendAppendEntriesResponse(response);
    }

    @Override
    void receiveRaftAppendEntriesResponse(RaftAppendEntriesResponse request) {
        logger.warn("Follower received a raft append entries response. That should not happen as only the leader should receive this message. it is ignored {}", request);
    }

    @Override
    void receiveHelloNotification(HelloNotification notification) {
        // if no leader has been elected we add the endpoint
        // only join remote peers if no remote peer is available, and obviously no leader has been elected
        if (this.config().autoDiscovery() && this.getLeaderId() == null) {
            if (UuidTools.notEquals(this.getLocalPeerId(), notification.sourcePeerId())) {
                this.remotePeers().join(notification.sourcePeerId());
            }
        }
        logger.trace("{} received hello notification {}", this.getLocalPeerId(), notification);
    }

    @Override
    void receiveEndpointNotification(EndpointStatesNotification notification) {
        // update the server endpoint states
        var remotePeers = this.remotePeers();
        if (notification.inactiveEndpointIds() != null) {
            boolean resetRequest = false;
            for (var it = notification.inactiveEndpointIds().iterator(); it.hasNext(); ) {
                var inactivePeerId = it.next();
                if (UuidTools.equals(this.getLocalPeerId(), inactivePeerId)) {
                    resetRequest = true;
                    continue;
                }
                var remotePeer = remotePeers.get(inactivePeerId);
                if (remotePeer == null || !remotePeer.active()) {
                    continue;
                }
                logger.warn("Detach endpoint {} at follower state at endpoint: {}", inactivePeerId, this.getLocalPeerId());
                remotePeers.detach(inactivePeerId);
            }
            if (resetRequest) {
                logger.debug("Reset is requested by a leader {} to this endpoint {} due to previous inactivity", notification.sourceEndpointId(), notification.destinationEndpointId());
                this.inactivatedLocalPeerId();
            }
        }
        if (UuidTools.notEquals(this.getLocalPeerId(), notification.sourceEndpointId())) {
            remotePeers.touch(notification.sourceEndpointId());
        }
        if (notification.activeEndpointIds() != null) {
            notification.activeEndpointIds()
                    .stream()
                    .filter(peerId -> UuidTools.notEquals(peerId, this.getLocalPeerId()))
                    .forEach(remotePeers::touch);
        }
        this.updated.set(Instant.now().toEpochMilli());
    }

    @Override
    public void run() {
        var config = this.config();
        var now = Instant.now().toEpochMilli();
        if (config.autoDiscovery()) {
            // in auto discovery mode we send hello notifications to discover the remote endpoints
            if (this.remotePeers().size() < 1) {
                if (config.sendingHelloTimeoutInMs() < now - this.sentHello.get()) {
                    var notification = new HelloNotification(this.getLocalPeerId(), null);
                    this.sendHelloNotification(notification);
                    this.sentHello.set(now);
                }
                // if we alone, there is not much point to start an election
                return;
            }
        }
        var elapsedInMs = now - this.updated.get();
        if (config.followerMaxIdleInMs() + this.extraWaitingTime < elapsedInMs) {
            logger.debug("{} is timed out to wait for append logs request (maxIdle: {}, elapsed: {}) Previously unsuccessful elections: {}, extra waiting time: {}", this.getLocalPeerId(), config.followerMaxIdleInMs(), elapsedInMs, this.timedOutElection, this.extraWaitingTime);
            this.elect(this.timedOutElection);
            return;
        }
    }

    @Override
    public RaftState getState() {
        return RaftState.FOLLOWER;
    }

}
