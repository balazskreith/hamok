package io.github.balazskreith.hamok.raft.events;

import java.util.UUID;

public record RaftVoteResponse(
        UUID sourcePeerId,
        UUID destinationPeerId,
        int term,
        boolean voteGranted
) {

}
