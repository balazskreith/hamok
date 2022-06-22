package com.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record UpdateEntriesRequest<K, V>(UUID requestId, Map<K, V> entries, UUID sourceEndpointId) {
    public static<U, R> OutboundRequestBuilder<U, R> builder() {
        return new OutboundRequestBuilder<U, R>();
    }

    public UpdateEntriesResponse<K, V> createResponse(Map<K, V> updatedEntries) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new UpdateEntriesResponse<K, V>(this.requestId, updatedEntries, this.sourceEndpointId);
    }

    public static class OutboundRequestBuilder<U, R> {
        private Map<U, R> entries;
        private UUID requestId = UUID.randomUUID();

        OutboundRequestBuilder() {

        }

        public OutboundRequestBuilder<U, R> setEntries(Map<U, R> entries) {
            this.entries = entries;
            return this;
        }

        public UpdateEntriesRequest<U, R> build() {
            Objects.requireNonNull(this.entries, "Cannot build outbound get entries request without keys");
            return new UpdateEntriesRequest<>(this.requestId, this.entries, null);
        }
    }
}
