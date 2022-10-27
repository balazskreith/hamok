package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record RemoveEntriesResponse<K, V>(
        UUID requestId,
        Map<K, V> removedEntries,
        UUID destinationEndpointId
) {

}
