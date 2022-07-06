package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record StorageSyncResponse(UUID requestId,
                                  Map<String, Message> storageUpdateNotifications,
                                  int commitIndex,
                                  UUID destinationId,
                                  boolean success,
                                  UUID leaderId,
                                  Integer sequence,
                                  Boolean lastMessage
) {

}
