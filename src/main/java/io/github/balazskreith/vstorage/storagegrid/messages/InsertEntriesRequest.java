package io.github.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record InsertEntriesRequest<K, V>(UUID requestId, Map<K, V> entries, UUID sourceEndpointId) {

    public InsertEntriesResponse<K, V> createResponse(Map<K, V> existingEntries) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new InsertEntriesResponse<K, V>(this.requestId, existingEntries, this.sourceEndpointId);
    }

}
