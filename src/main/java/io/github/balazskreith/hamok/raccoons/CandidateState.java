package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.raccoons.events.*;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class CandidateState extends AbstractState {
    private static final Logger logger = LoggerFactory.getLogger(CandidateState.class);

    private volatile long started = -1L;
    private AtomicInteger receivedVotes = new AtomicInteger(1);
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
    }


    @Override
    public RaftState getState() {
        return RaftState.CANDIDATE;
    }

    @Override
    public boolean submit(Message entry) {
        return false;
    }

    @Override
    void receiveVoteRequested(RaftVoteRequest request) {
        logger.debug("{} received Vote request. {}", this.getLocalPeerId(), request);
        var response = request.createResponse(false);
        this.sendVoteResponse(response);
    }

    @Override
    void receiveVoteResponse(RaftVoteResponse response) {
        if (electionTerm < response.term()) {
            // election should dismiss, case should be closed
            logger.info("Candidate received response from a higher term ({}) than the current election term ({}).", response.term(), this.electionTerm);
            this.follow();
            return;
        }
        if (response.term() < electionTerm) {
            logger.info("{} A vote response from a term smaller than the current is received: {}", this.getLocalPeerId(), response);
            return;
        }
        respondedRemotePeerIds.add(response.sourcePeerId());
        if (!response.voteGranted()) {
            return;
        }

        int receivedVotes = this.receivedVotes.incrementAndGet();
        int numberOfPeerIds = this.remotePeers().size() + 1; // +1, because of a local racoon!
        logger.info("{} Received vote for leadership: {}, number of peers: {}. a: {} ina: {}", this.getLocalPeerId(), receivedVotes, numberOfPeerIds,
                String.join(",", remotePeers().getActiveRemotePeerIds().stream().map(Object::toString).collect(Collectors.toList())),
                String.join(",", remotePeers().getInActiveRemotePeerIds().stream().map(Object::toString).collect(Collectors.toList())));
        if (numberOfPeerIds < receivedVotes * 2) {
            this.wonTheElection = true;
        }
    }

    @Override
    void receiveRaftAppendEntriesRequest(RaftAppendEntriesRequest request) {
        // a leader has been elected, let's go back to the follower state
        this.follow();
    }

    @Override
    void receiveRaftAppendEntriesResponse(RaftAppendEntriesResponse response) {
        logger.warn("{} cannot process a received response in candidate state {}", this.getLocalPeerId(), response);
    }

    @Override
    void receiveHelloNotification(HelloNotification notification) {
        logger.info("{} cannot process a received notification in candidate state {}", this.getLocalPeerId(), notification);
    }

    @Override
    void receiveEndpointNotification(EndpointStatesNotification notification) {
        logger.info("{} cannot process a received notification in candidate state {}", this.getLocalPeerId(), notification);
    }


    @Override
    public void run() {
        if (this.started < 0) {
            return;
        }
        var config = this.config();
        var elapsedTimeInMs = Instant.now().toEpochMilli() - this.started;
        if (this.wonTheElection) {
            logger.info("{} Won the election", this.getLocalPeerId());
            this.lead();
            return;
        }
        if (config.electionTimeoutInMs() < elapsedTimeInMs) {
            // election timeout
            logger.warn("{} Timeout occurred during the election process (electionTimeoutInMs: {}, elapsedTimeInMs: {}). This can be a result because of split vote. previously timed out elections: {}. elapsedTimeInMs: {}", this.getLocalPeerId(), config.electionTimeoutInMs(), elapsedTimeInMs, this.prevTimedOutElection, elapsedTimeInMs);
            if (config.autoDiscovery()) {
                // if we in auto discovery mode, then we need to investigate who replied and who odes not.
                var remotePeers = this.remotePeers();
                var remotePeerIds = remotePeers.getActiveRemotePeerIds();
                for (var remotePeerId : remotePeerIds) {
                    if (respondedRemotePeerIds.contains(remotePeerId)) {
                        continue;
                    }
                    remotePeers.detach(remotePeerId);
                }
            }
            this.follow(this.prevTimedOutElection + 1);
            return;
        }

    }

    void start() {
        Schedulers.computation().scheduleDirect(() -> {
            var config = this.config();
            var props = this.syncedProperties();
            var logs = this.logs();
            var remotePeers = this.remotePeers();
            for (var peerId : remotePeers.getActiveRemotePeerIds() ) {
                var request = new RaftVoteRequest(
                        peerId,
                        this.electionTerm,
                        config.id(),
                        logs.getNextIndex() - 1,
                        props.currentTerm.get()
                );
                this.sendVoteRequest(request);
            }
            this.started = Instant.now().toEpochMilli();
        });
    }
}
