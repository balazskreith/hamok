package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record RemoveEntriesRequest<K>(UUID requestId, Set<K> keys, UUID sourceEndpointId) {
    public static<U> OutboundRequestBuilder<U> builder() {
        return new OutboundRequestBuilder<U>();
    }

    public<V> RemoveEntriesResponse<K, V> createResponse(Map<K, V> removedEntries) {
        Objects.requireNonNull(this.sourceEndpointId, "Cannot create a response without a destination endpoint id");
        return new RemoveEntriesResponse<K, V>(this.requestId, removedEntries, this.sourceEndpointId);
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

        public RemoveEntriesRequest<U> build() {
            Objects.requireNonNull(this.keys, "Cannot build outbound get keys request without keys");
            return new RemoveEntriesRequest<U>(this.requestId, this.keys, null);
        }
    }
}
