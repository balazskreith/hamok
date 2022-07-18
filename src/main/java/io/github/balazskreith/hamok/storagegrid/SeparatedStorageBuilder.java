package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorageBuilder;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
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
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<SeparatedStorage<K, V>> storageBuiltListener = storage -> {};

    private Supplier<Codec<K, String>> keyCodecSupplier;
    private Supplier<Codec<V, String>> valueCodecSupplier;
    private Storage<K, V> actualStorage;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;

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
        this.storageId = value;
        this.storageEndpointBuilder.setStorageId(value);
        this.backupEndpointBuilder.setStorageId(value);
        return this;
    }

    public SeparatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setMaxCollectedStorageEvents(int value) {
        this.maxCollectedActualStorageEvents = value;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setMaxCollectedStorageTimeInMs(int value) {
        this.maxCollectedActualStorageTimeInMs = value;
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
        Objects.requireNonNull(this.storageId, "Cannot build without storage Id");
        var config = new SeparatedStorageConfig(
                this.storageId,
                this.maxCollectedActualStorageEvents,
                this.maxCollectedActualStorageTimeInMs
        );

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
            logger.info("Federated Storage {} is built with Concurrent Memory Storage ", storageEndpoint.getStorageId());
        }

        var backupMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var backupEndpoint = this.backupEndpointBuilder
                .setMessageSerDe(backupMessageSerDe)
                .setProtocol(BackupStorage.PROTOCOL_NAME)
                .build();
        this.storageEndpointBuiltListener.accept(backupEndpoint);
        var backups = new BackupStorageBuilder<K, V>()
                .withEndpoint(backupEndpoint)
                .build();

        var result = new SeparatedStorage<K, V>(this.actualStorage, storageEndpoint, backups, config);
        this.storageBuiltListener.accept(result);
        return result;
    }
}
