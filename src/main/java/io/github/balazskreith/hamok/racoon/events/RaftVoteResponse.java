package io.github.balazskreith.hamok.racoon.events;

import java.util.UUID;

public record RaftVoteResponse(
        UUID sourcePeerId,
        UUID destinationPeerId,
        int term,
        boolean voteGranted
) {

}
