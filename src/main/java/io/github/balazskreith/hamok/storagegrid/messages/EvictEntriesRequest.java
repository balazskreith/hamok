package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record EvictEntriesRequest<K>(UUID requestId, Set<K> keys, UUID sourceEndpointId) {
    public static<U> EvictEntriesRequest.OutboundRequestBuilder<U> builder() {
        return new EvictEntriesRequest.OutboundRequestBuilder<U>();
    }

    public<V> EvictEntriesResponse<K> createResponse() {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new EvictEntriesResponse<K>(this.requestId, this.sourceEndpointId);
    }


    public static class OutboundRequestBuilder<U> {
        private Set<U> keys;
        private UUID requestId = UUID.randomUUID();

        OutboundRequestBuilder() {

        }

        public EvictEntriesRequest.OutboundRequestBuilder<U> setKeys(Set<U> keys) {
            this.keys = keys;
            return this;
        }

        public EvictEntriesRequest<U> build() {
            Objects.requireNonNull(this.keys, "Cannot build outbound get keys request without keys");
            return new EvictEntriesRequest<U>(this.requestId, this.keys, null);
        }
    }
}
