package io.github.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record UpdateEntriesResponse<K, V>(UUID requestId, Map<K, V> entries, UUID destinationEndpointId) {
}
