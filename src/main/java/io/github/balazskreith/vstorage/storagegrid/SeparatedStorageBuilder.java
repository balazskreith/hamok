package io.github.balazskreith.vstorage.storagegrid;

import io.github.balazskreith.vstorage.Storage;
import io.github.balazskreith.vstorage.common.Depot;
import io.github.balazskreith.vstorage.mappings.Codec;
import io.github.balazskreith.vstorage.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.vstorage.storagegrid.backups.BackupStorage;
import io.github.balazskreith.vstorage.storagegrid.messages.StorageOpSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class SeparatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageBuilder.class);

    private final StorageEndpointBuilder<K, V> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private final StorageEndpointBuilder<K, V> backupEndpointBuilder = new StorageEndpointBuilder<>();


    private Supplier<Codec<K, String>> keyCodecSupplier;
    private Supplier<Codec<V, String>> valueCodecSupplier;
    private Storage<K, V> actualStorage;
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<SeparatedStorage<K, V>> storageBuiltListener = storage -> {};

    SeparatedStorageBuilder() {

    }

    SeparatedStorageBuilder<K, V> setStorageGrid(StorageGrid storageGrid) {
        this.storageEndpointBuilder.setStorageGrid(storageGrid);
        this.backupEndpointBuilder.setStorageGrid(storageGrid);
        return this;
    }

    SeparatedStorageBuilder<K, V> onEndpointBuilt(Consumer<StorageEndpoint<K, V>> listener) {
        this.storageEndpointBuiltListener = listener;
        return this;
    }

    SeparatedStorageBuilder<K, V> setMapDepotProvider(Supplier<Depot<Map<K, V>>> depotProvider) {
        this.storageEndpointBuilder.setMapDepotProvider(depotProvider);
        this.backupEndpointBuilder.setMapDepotProvider(depotProvider);
        return this;
    }

    SeparatedStorageBuilder<K, V> onStorageBuilt(Consumer<SeparatedStorage<K, V>> listener) {
        this.storageBuiltListener = listener;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setStorageId(String value) {
        this.storageEndpointBuilder.setStorageId(value);
        this.backupEndpointBuilder.setStorageId(value);
        return this;
    }

    public SeparatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }


    public SeparatedStorageBuilder<K, V> setKeyCodecSupplier(Supplier<Codec<K, String>> value) {
        this.keyCodecSupplier = value;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setValueCodecSupplier(Supplier<Codec<V, String>> value) {
        this.valueCodecSupplier = value;
        return this;
    }

    public SeparatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueCodecSupplier, "Codec for values must be defined");
        Objects.requireNonNull(this.keyCodecSupplier, "Codec for keys must be defined");

        var backupMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var backupEndpoint = this.backupEndpointBuilder
                .setMessageSerDe(backupMessageSerDe)
                .setProtocol(BackupStorage.PROTOCOL_NAME)
                .build();
        this.storageEndpointBuiltListener.accept(backupEndpoint);
        var backups = BackupStorage.<K, V>builder()
                .withEndpoint(backupEndpoint)
                .build();

        var actualMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var storageEndpoint = this.storageEndpointBuilder
                .setMessageSerDe(actualMessageSerDe)
                .setProtocol(SeparatedStorage.PROTOCOL_NAME)
                .build();
        this.storageEndpointBuiltListener.accept(storageEndpoint);
        if (this.actualStorage == null) {
            this.actualStorage = ConcurrentMemoryStorage.<K, V>builder()
                    .setId(storageEndpoint.getStorageId())
                    .build();
            logger.info("Separated Storage {} is built by using concurrent memory storage", storageEndpoint.getStorageId());
        }
        var result = new SeparatedStorage<K, V>(this.actualStorage, storageEndpoint, backups);
        this.storageBuiltListener.accept(result);
        return result;

    }

}
