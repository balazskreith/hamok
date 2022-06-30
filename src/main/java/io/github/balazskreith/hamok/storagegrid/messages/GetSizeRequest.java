package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Objects;
import java.util.UUID;

public record GetSizeRequest(UUID requestId, UUID sourceEndpointId) {

    public GetSizeResponse createResponse(int size) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new GetSizeResponse(this.requestId, size, this.sourceEndpointId);
    }
}
