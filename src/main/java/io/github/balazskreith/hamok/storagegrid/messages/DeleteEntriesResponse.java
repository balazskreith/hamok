package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Set;
import java.util.UUID;

public record DeleteEntriesResponse<K>(
        UUID requestId,
        Set<K> deletedKeys,
        UUID destinationEndpointId
) {

}
