package io.github.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record GetEntriesRequest<K>(Set<K> keys, UUID requestId, UUID sourceEndpointId) {

    public static<U> OutboundRequestBuilder<U> builder() {
        return new OutboundRequestBuilder<U>();
    }

    public<V> GetEntriesResponse<K, V> createResponse(Map<K, V> foundEntries) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new GetEntriesResponse<K, V>(this.requestId, foundEntries, this.sourceEndpointId);
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

        public GetEntriesRequest<U> build() {
            Objects.requireNonNull(this.keys, "Cannot build outbound get entries request without keys");
            return new GetEntriesRequest<>(this.keys, this.requestId, null);
        }
    }
}
