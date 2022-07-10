package io.github.balazskreith.hamok.storagegrid.backups;

/**
 * @param storageId The id of the storage the backup is tied to
 * @param storedEntries The number of entries saved locally in the backup storage
 * @param savedEntries The number of entries saved on remote endpoint backup storage
 */
public record BackupMetrics(
        String storageId,
        int storedEntries,
        int savedEntries
) {
}
