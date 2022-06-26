package io.github.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record StorageSyncRequest(UUID requestId, UUID sourceEndpointId) {

    public StorageSyncResponse createResponse(int commitIndex, Map<String, byte[]> storageUpdateNotifications, boolean success, UUID leaderId) {
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
