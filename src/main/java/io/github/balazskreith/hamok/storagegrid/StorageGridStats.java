package io.github.balazskreith.hamok.storagegrid;

public record StorageGridStats(
        int replicatedStorages,
        int separatedStorages,
        long sentBytes,
        long receivedBytes,
        int sentMessages,
        int receivedMessages,
        int pendingRequests,
        int pendingResponses
) {
}
