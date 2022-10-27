package io.github.balazskreith.hamok.storagegrid.backups;

public record BackupStorageConfig(
        int collectingTimeInMs,
        int collectingMaxItems
) {
}
