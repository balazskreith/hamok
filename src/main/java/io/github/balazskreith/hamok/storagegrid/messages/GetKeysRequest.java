package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record GetKeysRequest(UUID requestId, UUID sourceEndpointId) {

    public<K> GetKeysResponse<K> createResponse(Set<K> keys) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new GetKeysResponse(this.requestId, keys, this.sourceEndpointId);
    }
}
