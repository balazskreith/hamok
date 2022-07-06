package io.github.balazskreith.hamok.raccoons.events;

import java.util.UUID;

public record RaftAppendEntriesResponse(
        UUID sourcePeerId,
        UUID destinationPeerId,
        int term,
        boolean success,
        int peerNextIndex,
        boolean processed
) {


}
