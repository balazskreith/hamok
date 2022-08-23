package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.raccoons.events.*;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

class CandidateState extends AbstractState {
    private static final Logger logger = LoggerFactory.getLogger(CandidateState.class);

    private volatile long started = -1L;
    private Set<UUID> notifiedRemotePeers = Collections.synchronizedSet(new HashSet<>());
    private Set<UUID> receivedVotes = Collections.synchronizedSet(new HashSet<>());
    private final int electionTerm;
    private volatile boolean wonTheElection = false;
    private final int prevTimedOutElection;
    private final Set<UUID> respondedRemotePeerIds = Collections.synchronizedSet(new HashSet<>());

    CandidateState(
            Raccoon racoon,
            int prevTimedOutElection
    ) {
        super(racoon);
        this.prevTimedOutElection = prevTimedOutElection;
        this.electionTerm = this.syncedProperties().currentTerm.get() + 1;
        this.setActualLeaderId(null);
    }


    @Override
    public RaftState getState() {
        return RaftState.CANDIDATE;
    }

    @Override
    public boolean submit(Models.Message entry) {
        logger.debug("{} got a submit for message {}", this.getClass().getSimpleName(), entry);
        return false;
    }

    @Override
    void receiveVoteRequested(RaftVoteRequest request) {
        logger.trace("{} received Vote request. {}", this.getLocalPeerId(), request);
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

        if (electionTerm < response.term()) {
            // election should dismiss, case should be closed
            logger.info("Candidate received response from a higher term ({}) than the current election term ({}).", response.term(), this.electionTerm);
            this.follow();
            return;
        }
        if (response.term() < electionTerm) {
            logger.warn("A vote response from a term smaller than the current is received: {}", response);
            return;
        }
        respondedRemotePeerIds.add(response.sourcePeerId());
        if (!response.voteGranted()) {
            return;
        }
        this.receivedVotes.add(response.sourcePeerId());

        int numberOfPeerIds = this.remotePeers().size() + 1; // +1, because of a local raccoon!
        logger.debug("Received vote for leadership: {}, number of peers: {}. activeRemoteEndpointIds: {}", receivedVotes.size(), numberOfPeerIds,
                String.join(",", remotePeers().getRemotePeerIds().stream().map(Object::toString).collect(Collectors.toList()))
        );
        if (numberOfPeerIds < receivedVotes.size() * 2) {
            this.wonTheElection = true;
        }
        else if (this.notifiedRemotePeers.size() < numberOfPeerIds) {
            // might happens that a new remote endpoint has been discovered since we started the process.
            // in this case we resent the vote requests
            for (var it = remotePeers.iterator(); it.hasNext(); ) {
                var remotePeer = it.next();
                if (!notifiedRemotePeers.contains(remotePeer.id())) {
                    var request = new RaftVoteRequest(
                            remotePeer.id(),
                            this.electionTerm,
                            this.config().id(),
                            logs().getNextIndex() - 1,
                            syncedProperties().currentTerm.get()
                    );
                    this.sendVoteRequest(request);
                    logger.info("Send vote request to {} after the candidacy has been started", request);
                }
            }
        }
    }

    @Override
    void receiveRaftAppendEntriesRequestChunk(RaftAppendEntriesRequestChunk request) {
        // a leader has been elected, let's go back to the follower state
        this.follow();
    }

    @Override
    void receiveRaftAppendEntriesResponse(RaftAppendEntriesResponse response) {
        logger.warn("{} cannot process a received response in candidate state {}", this.getLocalPeerId(), response);
    }

    @Override
    void receiveHelloNotification(HelloNotification notification) {
        logger.debug("{} cannot process a received notification in candidate state {}", this.getLocalPeerId(), notification);
    }

    @Override
    void receiveEndpointNotification(EndpointStatesNotification notification) {
        logger.debug("{} cannot process a received notification in candidate state {}", this.getLocalPeerId(), notification);
    }


    @Override
    public void run() {
        if (this.started < 0) {
            return;
        }
        var config = this.config();
        var elapsedTimeInMs = Instant.now().toEpochMilli() - this.started;
        if (this.wonTheElection) {
            logger.debug("{} Won the election", this.getLocalPeerId());
            this.lead();
            return;
        }
        if (config.electionTimeoutInMs() < elapsedTimeInMs) {
            // election timeout
            logger.warn("{} Timeout occurred during the election process (electionTimeoutInMs: {}, elapsedTimeInMs: {}, respondedRemotePeerIds: {}). This can be a result because of split vote. previously timed out elections: {}. elapsedTimeInMs: {}", this.getLocalPeerId(), config.electionTimeoutInMs(), elapsedTimeInMs, this.respondedRemotePeerIds.size(), this.prevTimedOutElection, elapsedTimeInMs);
            this.follow(this.prevTimedOutElection + 1);
            return;
        }

    }

    void start() {
        // voting for myself!
        this.receivedVotes.add(this.config().id());

        Schedulers.computation().scheduleDirect(() -> {
            var config = this.config();
            var props = this.syncedProperties();
            var logs = this.logs();
            var remotePeers = this.remotePeers();
            for (var peerId : remotePeers.getRemotePeerIds() ) {
                var request = new RaftVoteRequest(
                        peerId,
                        this.electionTerm,
                        config.id(),
                        logs.getNextIndex() - 1,
                        props.currentTerm.get()
                );
                this.sendVoteRequest(request);
                logger.info("Send vote request to {}", request);
            }
            this.started = Instant.now().toEpochMilli();
        });
    }

    @Override
    protected void sendVoteRequest(RaftVoteRequest request) {
        this.notifiedRemotePeers.add(request.peerId());
        super.sendVoteRequest(request);
    }
}
