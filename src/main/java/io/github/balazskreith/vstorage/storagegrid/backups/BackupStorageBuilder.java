package io.github.balazskreith.vstorage.storagegrid.backups;

import io.github.balazskreith.vstorage.storagegrid.StorageEndpoint;

import java.util.Objects;

public class BackupStorageBuilder<K, V> {

    private StorageEndpoint<K, V> endpoint;

    public BackupStorageBuilder<K, V> withEndpoint(StorageEndpoint<K, V> endpoint) {
        this.endpoint = endpoint;
        return this;
    }


    public BackupStorage<K, V> build() {
        Objects.requireNonNull(this.endpoint, "Cannot build backup storage without endpoint");
        var result = new ConcurrentMemoryBackupStorage<K, V>(this.endpoint);
        return result;
    }
}
