package io.github.balazskreith.hamok.racoon.events;

import java.util.UUID;

public record RaftVoteRequest(
        UUID peerId,
        int term,
        UUID candidateId,
        int lastLogIndex,
        int lastLogTerm
) {
    public RaftVoteResponse createResponse(boolean voteGranted) {
        return new RaftVoteResponse(this.peerId, this.candidateId, this.term, voteGranted);
    }
}
