package io.github.balazskreith.hamok.raccoons.events;

import java.util.UUID;

public record RaftVoteResponse(
        UUID sourcePeerId,
        UUID destinationPeerId,
        int term,
        boolean voteGranted
) {

}
