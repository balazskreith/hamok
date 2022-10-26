package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Objects;
import java.util.UUID;

public record ClearEntriesRequest(UUID requestId, UUID sourceEndpointId) {

    public ClearEntriesResponse createResponse() {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new ClearEntriesResponse(this.requestId, sourceEndpointId);
    }
}
