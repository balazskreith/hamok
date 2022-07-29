package io.github.balazskreith.hamok.storagegrid;

public record FederatedMultiStorageConfig(
        String storageId,
        int maxCollectedActualStorageEvents,
        int maxCollectedActualStorageTimeInMs,
        int iteratorBatchSize
) {
}
