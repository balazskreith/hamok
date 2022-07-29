package io.github.balazskreith.hamok.storagegrid;

public record ReplicatedStorageConfig(
        String storageId,
        int maxCollectedActualStorageEvents,
        int maxCollectedActualStorageTimeInMs
) {
}
