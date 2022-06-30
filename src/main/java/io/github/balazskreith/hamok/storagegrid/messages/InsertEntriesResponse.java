package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record InsertEntriesResponse<K, V>(UUID requestId, Map<K, V> existingEntries, UUID destinationEndpointId) {
}
