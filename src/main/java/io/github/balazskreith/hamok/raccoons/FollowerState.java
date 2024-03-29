package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.Utils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

class FollowerState extends AbstractState {

    private static final Logger logger = LoggerFactory.getLogger(FollowerState.class);
    private AtomicLong updated = new AtomicLong(Instant.now().toEpochMilli());
    private AtomicLong voted = new AtomicLong(0);
    private AtomicLong sentHello = new AtomicLong(0);
    private Map<UUID, RaftAppendEntriesRequest> pendingRequests = new ConcurrentHashMap<>();
    private volatile boolean syncRequested = false;
    private volatile int timedOutElection;
    private int extraWaitingTime = 0;
    private volatile boolean receivedEndpointNotification = false;
    private volatile int shouldLogOnceFlags = 0;
    private volatile boolean unsyncRaftLogsLogged = false;

    FollowerState(Raccoon base) {
        this(base, 0);
    }

    FollowerState(Raccoon base, int timedOutElection) {
        super(base);
        this.timedOutElection = timedOutElection;
        if (0 < timedOutElection) {
            var config = this.config();
            var random = new SecureRandom();
            int extraWaitingTimeInMs = random.nextInt(1 + 10 * config.electionTimeoutInMs());
            for (int i = 0; i < Math.min(30, timedOutElection); ++i) {
                int multiplier = random.nextInt(10);
                extraWaitingTimeInMs += random.nextInt(1 + multiplier * config.heartbeatInMs());
            }
            this.extraWaitingTime = extraWaitingTimeInMs;
        }
        var props = this.syncedProperties();
        props.votedFor.set(null);
        this.setActualLeaderId(null);
    }

    @Override
    void start() {
        // nothing we do here
    }

    @Override
    public boolean submit(Models.Message entry) {
        logger.debug("{} got a submit for message {}", this.getClass().getSimpleName(), entry);
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
        if (voteGranted) {
            // when we vote for this candidate for the first time,
            // let's restart the timer, so we wait a bit more to give time to win the election before we run for presidency.
            this.updated.set(Instant.now().toEpochMilli());
        } else {
            // maybe we already voted for the candidate itself?
            voteGranted = UuidTools.equals(props.votedFor.get(), request.candidateId());
        }
        var response = request.createResponse(voteGranted);
        logger.info("{} send a vote response {}.", this.getLocalPeerId(), response);
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
    void receiveRaftAppendEntriesRequestChunk(RaftAppendEntriesRequestChunk requestChunk) {
        var props = this.syncedProperties();
        var currentTerm = props.currentTerm.get();
        if (requestChunk.term() < currentTerm) {
            logger.warn("{} Append entries request appeared from a previous term. currentTerm: {}, received entries request term: {}", this.getLocalPeerId(), currentTerm, requestChunk.term());
            var response = requestChunk.createResponse(false, -1, false);
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

        // set the actual leader
        if (requestChunk.leaderId() != null) {
            this.setActualLeaderId(requestChunk.leaderId());
        }

        // let's touch the leader (wierd sentence and I don't want to elaborate)
        if (UuidTools.notEquals(this.getLocalPeerId(), requestChunk.peerId())) {
            this.remotePeers().touch(requestChunk.peerId());
        }

        var logs = this.logs();
        if (requestChunk.entry() == null && requestChunk.sequence() == 0) {
            if (requestChunk.lastMessage() == false) {
                logger.warn("{} Entries cannot be null if it is a part of chunks and thats not the last message", this.getLocalPeerId());
                var response = requestChunk.createResponse(false, -1, true);
                this.sendAppendEntriesResponse(response);
                return;
            }
            // that was a keep alive message
            if (!this.syncRequested) {
                // try to update if a sync is not in progress
                this.updateCommitIndex(requestChunk.leaderCommit());
            }
            var response = requestChunk.createResponse(true, logs().getNextIndex(), true);
            this.sendAppendEntriesResponse(response);
            return;
        }

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

        logger.trace("Received RaftAppendEntriesRequest {} Entries: {}", request, Utils.firstNonNull(request.entries(), Collections.emptyList()).size());
        if (this.syncRequested) {
            if ((this.shouldLogOnceFlags & 1) == 0) {
                logger.warn("Commit sync is being executed at the moment");
                this.shouldLogOnceFlags += 1;
            }

            // until we do not sync we cannot process and go forward with our index
            var response = requestChunk.createResponse(
                    true,
                    logs.getNextIndex(),
                    true
            );
            this.sendAppendEntriesResponse(response);
            return;
        } else if ((this.shouldLogOnceFlags & 1) == 1) {
            this.shouldLogOnceFlags -= 1;
        }

        if (logs.getNextIndex() < request.leaderNextIndex() - request.entries().size()) {
            if (!this.syncRequested) {
                logger.warn("The next index is {}, and the leader index is: {}, the provided entries are: {}. It is insufficient to close the gap for this node. Execute sync request is necessary from the leader to request and the timeout of the raft logs should be large enough to close the gap after the sync.",
                        logs.getNextIndex(),
                        request.leaderNextIndex(),
                        request.entries().size()
                );
                this.requestStorageSync().whenComplete((res, err) -> {
                    this.syncRequested = false;
                });
            }
            if (!this.receivedEndpointNotification) {

            }
            // we send success and processed response as the problem is not with the request,
            // but we do not change our next index because we cannot process it momentary due to not synced endpoint
            var response = requestChunk.createResponse(true, logs.getNextIndex(), true);
            this.sendAppendEntriesResponse(response);
            return;
        } else if (this.unsyncRaftLogsLogged) {
            this.unsyncRaftLogsLogged = false;
        }

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
        this.updateCommitIndex(requestChunk.leaderCommit());
        var response = requestChunk.createResponse(success, logs.getNextIndex(), true);
//            logger.info("{} sending {}", this.getId(), response);
        this.sendAppendEntriesResponse(response);
    }

    @Override
    void receiveRaftAppendEntriesResponse(RaftAppendEntriesResponse request) {
        if (request.destinationPeerId() == null) {
            return;
        }
        if (UuidTools.equals(request.destinationPeerId(), this.getLocalPeerId())) {
            logger.warn("Follower received a raft append entries response. That should not happen as only the leader should receive this message. it is ignored {}", request);
            return;
        }
        if (this.getLeaderId() != null || UuidTools.equals(request.destinationPeerId(), this.getLeaderId())) {
            return;
        }
        if (this.syncedProperties().currentTerm.get() < request.term()) {
            logger.warn("Follower received a raft append response, and the leader term is higher than this, so we change it");
            this.setActualLeaderId(request.destinationPeerId());
        }
    }

    @Override
    void receiveHelloNotification(HelloNotification notification) {
        // if auto discovery is on and no leader has been elected we add the endpoint
        logger.trace("{} received hello notification {}", this.getLocalPeerId(), notification);
    }

    @Override
    void receiveEndpointNotification(EndpointStatesNotification notification) {
        // update the server endpoint states
        if (notification.term() < syncedProperties().currentTerm.get()) {
            logger.warn("Received endpoint state notification from a node ({}) has lower term than this (remoteTerm: {}, this term: {}). The reporting node should go into a follower mode",
                    notification.sourceEndpointId(),
                    notification.term(),
                    syncedProperties().currentTerm.get()
            );
            return;
        }
        this.receivedEndpointNotification = true;
//        logger.info("Received endpoint state {}", notification);
//        var remotePeers = this.remotePeers();

        if (this.getLeaderId() == null) {
            this.setActualLeaderId(notification.sourceEndpointId());
        }
        // TODO: compare our active endpoints with the remote one and emit events accordingly

        this.updated.set(Instant.now().toEpochMilli());
    }

    @Override
    public void run() {
        var config = this.config();
        var now = Instant.now().toEpochMilli();
        if (this.getLeaderId() == null || !this.receivedEndpointNotification) {
            // if we don't know any leader, or we have not received endpoint state notification and
            // since the sentHello is -1 by default that ensures hello is sent when state change
            // happens, which if we have a leader makes it to send the endpoint state notification
            if (config.sendingHelloTimeoutInMs() < now - this.sentHello.get()) {
                var notification = new HelloNotification(this.getLocalPeerId(), null);
                this.sendHelloNotification(notification);
                logger.debug("Sent hello message {}", notification);
                this.sentHello.set(now);
            }
        }
        var updated = this.updated.get();
        var elapsedInMs = now - updated;
        if (config.followerMaxIdleInMs() + this.extraWaitingTime < elapsedInMs) {
            // we don't know a leader at this point
            this.setActualLeaderId(null);
            if (this.remotePeers().size() < 1) {
                // if we alone, there is not much point to start an election
                return;
            }

            logger.debug("{} is timed out to wait for append logs request (maxIdle: {}, elapsed: {}) Previously unsuccessful elections: {}, extra waiting time: {}", this.getLocalPeerId(), config.followerMaxIdleInMs(), elapsedInMs, this.timedOutElection, this.extraWaitingTime);
            this.elect(this.timedOutElection);
            return;
        }
    }

    @Override
    public RaftState getState() {
        return RaftState.FOLLOWER;
    }

    private void updateCommitIndex(int leaderCommitIndex) {
        var logs = logs();
        if (leaderCommitIndex <= logs.getCommitIndex()) {
            return;
        }
        var expectedCommitIndex = Math.min(logs.getNextIndex() - 1, leaderCommitIndex);
        var committedLogEntries = logs.commitUntil(expectedCommitIndex);
        committedLogEntries.forEach(this::commitLogEntry);
    }

}
