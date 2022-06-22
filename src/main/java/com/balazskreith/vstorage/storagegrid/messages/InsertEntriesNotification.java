package com.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public record InsertEntriesNotification<K, V>(Map<K, V> entries, UUID sourceEndpointId, UUID destinationEndpointId) {
    public static<U, R> OutboundNotificationBuilder<U, R> builder() {
        return new OutboundNotificationBuilder<U, R>();
    }

    public static class OutboundNotificationBuilder<U, R> {
        private Map<U, R> entries;
        private UUID destinationEndpointId;

        OutboundNotificationBuilder() {

        }

        public OutboundNotificationBuilder<U, R> setEntries(Map<U, R> entries) {
            this.entries = entries;
            return this;
        }

        public OutboundNotificationBuilder<U, R> setDestinationEndpointId(UUID destinationEndpointId) {
            this.destinationEndpointId = destinationEndpointId;
            return this;
        }

        public InsertEntriesNotification<U, R> build() {
            Objects.requireNonNull(this.entries, "Cannot build outbound get entries request without keys");
            return new InsertEntriesNotification<>(this.entries, null, this.destinationEndpointId);
        }
    }
}
