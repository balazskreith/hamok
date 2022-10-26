package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.UUID;

public record ClearEntriesResponse(UUID requestId, UUID destinationEndpointId) {

}
