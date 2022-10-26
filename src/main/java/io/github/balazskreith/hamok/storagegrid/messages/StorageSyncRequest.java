package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.UUID;

public record StorageSyncRequest(
        UUID requestId,
        UUID sourceEndpointId,
        UUID leaderId
        ) {

    public StorageSyncResponse createResponse(
            UUID leaderId,
            int numberOfLogs,
            int lastApplied,
            int commitIndex
            ) {
        return new StorageSyncResponse(
                this.requestId,
                this.sourceEndpointId,
                leaderId,
                numberOfLogs,
                lastApplied,
                commitIndex
        );
    }
}
