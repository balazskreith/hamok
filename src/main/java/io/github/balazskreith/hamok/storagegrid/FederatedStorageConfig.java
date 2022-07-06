package io.github.balazskreith.hamok.storagegrid;

public record FederatedStorageConfig(
        String storageId,
        int maxCollectedActualStorageEvents,
        int maxCollectedActualStorageTimeInMs,
        int iteratorBatchSize,
        int maxMessageKeys,
        int maxMessageValues
) {
}
