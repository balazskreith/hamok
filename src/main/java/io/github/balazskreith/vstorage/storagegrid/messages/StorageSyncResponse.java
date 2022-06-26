package io.github.balazskreith.vstorage.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record StorageSyncResponse(UUID requestId,
                                  Map<String, byte[]> storageUpdateNotifications,
                                  int commitIndex,
                                  UUID destinationId,
                                  boolean success,
                                  UUID leaderId
) {

}
