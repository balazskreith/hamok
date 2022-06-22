package com.balazskreith.vstorage.storagegrid.messages;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record DeleteEntriesRequest<K>(UUID requestId, Set<K> keys, UUID sourceEndpointId) {
    public static<U> OutboundRequestBuilder<U> builder() {
        return new OutboundRequestBuilder<U>();
    }

    public<V> DeleteEntriesResponse<K> createResponse(Set<K> deletedKeys) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new DeleteEntriesResponse<K>(this.requestId, deletedKeys, this.sourceEndpointId);
    }



    public static class OutboundRequestBuilder<U> {
        private Set<U> keys;
        private UUID requestId = UUID.randomUUID();

        OutboundRequestBuilder() {

        }

        public OutboundRequestBuilder<U> setKeys(Set<U> keys) {
            this.keys = keys;
            return this;
        }

        public DeleteEntriesRequest<U> build() {
            Objects.requireNonNull(this.keys, "Cannot build outbound get keys request without keys");
            return new DeleteEntriesRequest<U>(this.requestId, this.keys, null);
        }
    }
}
