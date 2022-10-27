package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.UUID;

public record EvictEntriesResponse<K>(UUID requestId, UUID destinationEndpointId) {

}
