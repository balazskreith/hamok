package io.github.balazskreith.hamok.storagegrid;

public record SeparatedStorageConfig(
        String storageId,
        int maxCollectedActualStorageEvents,
        int maxCollectedActualStorageTimeInMs,
        int iteratorBatchSize,
        int maxMessageKeys,
        int maxMessageValues
) {
}
