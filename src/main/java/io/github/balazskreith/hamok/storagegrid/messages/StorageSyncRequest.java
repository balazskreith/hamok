package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record StorageSyncRequest(UUID requestId, UUID sourceEndpointId, UUID leaderId) {

    public StorageSyncResponse createResponse(int commitIndex, Map<String, Message> storageUpdateNotifications, boolean success, UUID leaderId) {
        return new StorageSyncResponse(
                this.requestId,
                storageUpdateNotifications,
                commitIndex,
                this.sourceEndpointId,
                success,
                leaderId
        );
    }
}
