package com.balazskreith.vstorage.storagegrid.messages;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public record EvictEntriesNotification<K>(Set<K> keys, UUID sourceEndpointId, UUID destinationEndpointId) {
    public static<U> OutboundNotificationBuilder<U> builder() {
        return new OutboundNotificationBuilder<U>();
    }

    public static class OutboundNotificationBuilder<U> {
        private Set<U> keys;
        private UUID destinationEndpointId;

        OutboundNotificationBuilder() {

        }

        public OutboundNotificationBuilder<U> setKeys(Set<U> keys) {
            this.keys = keys;
            return this;
        }

        public OutboundNotificationBuilder<U> setDestinationEndpointId(UUID destinationEndpointId) {
            this.destinationEndpointId = destinationEndpointId;
            return this;
        }

        public EvictEntriesNotification<U> build() {
            Objects.requireNonNull(this.keys, "Cannot build outbound get entries request without keys");
            return new EvictEntriesNotification<>(this.keys, null, this.destinationEndpointId);
        }
    }
}
