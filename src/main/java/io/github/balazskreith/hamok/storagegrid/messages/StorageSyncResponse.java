package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.UUID;

public record StorageSyncResponse(UUID requestId,
                                  UUID destinationId,
                                  UUID leaderId,
                                  int numberOfLogs,
                                  int lastApplied,
                                  int commitIndex
) {

}
