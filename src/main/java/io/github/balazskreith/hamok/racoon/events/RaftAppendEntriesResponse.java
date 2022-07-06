package io.github.balazskreith.hamok.racoon.events;

import java.util.UUID;

public record RaftAppendEntriesResponse(
        UUID sourcePeerId,
        UUID destinationPeerId,
        int term,
        boolean success,
        int peerNextIndex
) {


}
