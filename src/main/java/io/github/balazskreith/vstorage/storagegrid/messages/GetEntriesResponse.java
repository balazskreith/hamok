package io.github.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record GetEntriesResponse<K, V>(UUID requestId, Map<K, V> foundEntries, UUID destinationEndpointId) {

}
