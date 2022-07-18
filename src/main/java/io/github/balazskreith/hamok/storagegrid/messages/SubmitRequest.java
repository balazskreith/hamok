package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Objects;
import java.util.UUID;

public record SubmitRequest(UUID requestId, UUID destinationId, byte[] entry) {
    public static OutboundNotificationBuilder builder() {
        return new OutboundNotificationBuilder();
    }

    public static class OutboundNotificationBuilder {
        private UUID requestId;
        private UUID destinationId;
        private byte[] entry;

        OutboundNotificationBuilder() {

        }

        public OutboundNotificationBuilder setRequestId(UUID requestId) {
            this.requestId = requestId;
            return this;
        }

        public OutboundNotificationBuilder setDestinationId(UUID destinationId) {
            this.destinationId = destinationId;
            return this;
        }

        public OutboundNotificationBuilder setEntry(byte[] entry) {
            this.entry = entry;
            return this;
        }


        public SubmitRequest build() {
            Objects.requireNonNull(this.requestId, "Cannot build a request without a requestId");
            Objects.requireNonNull(this.destinationId, "Cannot build outbound submit notification without destinationId");
            Objects.requireNonNull(this.entry, "Cannot build outbound submit notification without entry");
            return new SubmitRequest(this.requestId, this.destinationId, this.entry);
        }
    }

    public SubmitResponse createResponse(UUID destinationId, boolean success, UUID leaderId) {
        return new SubmitResponse(
                this.requestId,
                destinationId,
                success,
                leaderId
        );
    }
}
