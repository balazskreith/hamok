package io.github.balazskreith.vstorage.raft;

import io.github.balazskreith.vstorage.common.JsonUtils;
import io.github.balazskreith.vstorage.raft.events.RaftVoteRequest;
import io.reactivex.rxjava3.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

class Candidate extends AbstractActor {
    private static final Logger logger = LoggerFactory.getLogger(Candidate.class);

    private final CompletableFuture<Boolean> election = new CompletableFuture<>();

    private AtomicInteger receivedVotes = new AtomicInteger(1);
    private final int electionTerm;
    Candidate(
            RxRaft raft
    ) {
        super(raft);
        this.electionTerm = raft.syncedProperties().currentTerm.get() + 1;
        this.addDisposable(Disposable.fromRunnable(() -> {
            this.raft = null;
        }));
        this.onVoteResponse(response -> {
            if (election.isDone() || election.isCancelled()) {
                logger.info("{} VoteResponse received to a cancelled or already done election. term: {}", this.getId(), this.electionTerm);
                return;
            }
            if (response.term() < electionTerm) {
                logger.info("{} A vote response from a term smaller than the current is received: {}", this.getId(), JsonUtils.objectToString(response));
                return;
            }
            if (electionTerm < response.term()) {
                // election should dismiss, case should be closed
                this.election.complete(false);
                return;
            }
            if (!response.voteGranted()) {
                return;
            }
            int receivedVotes = this.receivedVotes.incrementAndGet();
            logger.info("{} Received vote for leadership: {}", this.getId(), receivedVotes);
            int numberOfPeerIds = this.raft.syncedProperties().peerIds.size() + 1;
            if (numberOfPeerIds < receivedVotes * 2) {
                this.election.complete(true);
            }
        });
    }


    @Override
    public RaftState getState() {
        return RaftState.CANDIDATE;
    }

    @Override
    public void start() {
        this.addDisposable(this.raft.scheduler().scheduleDirect(this::process));
    }

    @Override
    public void stop() {
        if (!this.isDisposed()) {
            this.raft.syncedProperties().votedFor.set(null);
        }
        super.stop();
    }

    @Override
    public void removedPeerId(UUID peerId) {

    }

    @Override
    public Integer submit(byte[] entry) {
        return null;
    }

    private void process() {
        if (this.isDisposed()) {
            logger.warn("{} {} state is being tried to be processed", this.getId(), this.getState().name());
            return;
        }
        var config = this.raft.config();
        var props = this.raft.syncedProperties();
        var logs = this.raft.logs();
        for (var it = props.peerIds.iterator(); it.hasNext(); ) {
            var peerId = it.next();
            var request = new RaftVoteRequest(
                    peerId,
                    this.electionTerm,
                    config.id(),
                    logs.getNextIndex() - 1,
                    props.currentTerm.get()
            );
            this.sendVoteRequest(request);
        }

        Boolean iThinkWeWon = null;
        try {
            iThinkWeWon = this.election.get(config.electionMinTimeoutInMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.warn("{} election is interrupted. {}", this.getId(), e);
            this.raft.changeActor(new Follower(this.raft));
            return;
        } catch (ExecutionException e) {
            logger.warn("{} Error occurred during the election. {}", this.getId(), e);
            this.raft.changeActor(new Follower(this.raft));
            return;
        } catch (TimeoutException e) {
            logger.warn("{} Timeout occurred during the election process. If you see this message many times consecutively consider to increase the election timeout or follower timeout offset. {}", this.getId());
            this.raft.changeActor(new Follower(this.raft));
            return;
        }
        if (Boolean.FALSE.equals(iThinkWeWon)) {
            // No Trump, you did not
            this.raft.changeActor(new Follower(this.raft));
        } else {
            this.raft.syncedProperties().currentTerm.set(this.electionTerm);
            this.raft.changeActor(new Leader(this.raft));
        }
    }


}
