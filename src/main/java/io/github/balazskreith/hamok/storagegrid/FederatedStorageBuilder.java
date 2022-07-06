package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorageBuilder;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class FederatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(FederatedStorageBuilder.class);

    private final StorageEndpointBuilder<K, V> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private final StorageEndpointBuilder<K, V> backupEndpointBuilder = new StorageEndpointBuilder<>();
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<FederatedStorage<K, V>> storageBuiltListener = storage -> {};
    private StorageGrid grid;

    private Supplier<Codec<K, byte[]>> keyCodecSupplier;
    private Supplier<Codec<V, byte[]>> valueCodecSupplier;
    private BinaryOperator<V> mergeOperator;
    private Storage<K, V> actualStorage;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;
    private int iteratorBatchSize = 300;
    private int maxMessageKeys = 10000;
    private int maxMessageValues = 1000;

    FederatedStorageBuilder() {
        Supplier<Depot<Map<K, V>>> mergedMapDepotProvider = () -> MapUtils.makeMergedMapDepot(this.mergeOperator);
        this.storageEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.backupEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
    }

    FederatedStorageBuilder<K, V> setStorageGrid(StorageGrid grid) {
        this.grid = grid;
        this.storageEndpointBuilder.setStorageGrid(grid);
        this.backupEndpointBuilder.setStorageGrid(grid);
        return this;
    }

    FederatedStorageBuilder<K, V> onEndpointBuilt(Consumer<StorageEndpoint<K, V>> listener) {
        this.storageEndpointBuiltListener = listener;
        return this;
    }

    FederatedStorageBuilder<K, V> onStorageBuilt(Consumer<FederatedStorage<K, V>> listener) {
        this.storageBuiltListener = listener;
        return this;
    }

    public FederatedStorageBuilder<K, V> setMaxMessageKeys(int value) {
        this.maxMessageKeys = value;
        return this;
    }

    public FederatedStorageBuilder<K, V> setMaxMessageValues(int value) {
        this.maxMessageValues = value;
        return this;
    }

    public FederatedStorageBuilder<K, V> setMergeOperator(Supplier<BinaryOperator<V>> mergeOpProvider) {
        Supplier<Depot<Map<K, V>>> mergedMapDepotProvider = () -> MapUtils.makeMergedMapDepot(mergeOpProvider.get());
        this.storageEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.backupEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.mergeOperator = mergeOpProvider.get();
        return this;
    }

    public FederatedStorageBuilder<K, V> setIteratorBatchSize(int value) {
        this.iteratorBatchSize = value;
        return this;
    }


    public FederatedStorageBuilder<K, V> setStorageId(String value) {
        this.storageId = value;
        this.backupEndpointBuilder.setStorageId(value);
        this.storageEndpointBuilder.setStorageId(value);
        return this;
    }

    public FederatedStorageBuilder<K, V> setMaxCollectedStorageEvents(int value) {
        this.maxCollectedActualStorageEvents = value;
        return this;
    }

    public FederatedStorageBuilder<K, V> setMaxCollectedStorageTimeInMs(int value) {
        this.maxCollectedActualStorageTimeInMs = value;
        return this;
    }

    public FederatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }

    public FederatedStorageBuilder<K, V> setKeyCodecSupplier(Supplier<Codec<K, byte[]>> value) {
        this.keyCodecSupplier = value;
        return this;
    }

    public FederatedStorageBuilder<K, V> setValueCodecSupplier(Supplier<Codec<V, byte[]>> value) {
        this.valueCodecSupplier = value;
        return this;
    }

    public FederatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueCodecSupplier, "Codec for values must be defined");
        Objects.requireNonNull(this.keyCodecSupplier, "Codec for keys must be defined");
        Objects.requireNonNull(this.mergeOperator, "Cannot build without merge operator");
        Objects.requireNonNull(this.storageId, "Cannot build without storage Id");
        var config = new FederatedStorageConfig(
                this.storageId,
                this.maxCollectedActualStorageEvents,
                this.maxCollectedActualStorageTimeInMs,
                this.iteratorBatchSize,
                this.maxMessageKeys,
                this.maxMessageValues
        );

        var actualMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var storageEndpoint = this.storageEndpointBuilder
                .setDefaultResolvingEndpointIdsSupplier(this.grid::getRemoteEndpointIds)
                .setMessageSerDe(actualMessageSerDe)
                .setProtocol(FederatedStorage.PROTOCOL_NAME)
                .build();
        storageEndpoint.requestsDispatcher().subscribe(this.grid::send);
        this.storageEndpointBuiltListener.accept(storageEndpoint);
        if (this.actualStorage == null) {
            this.actualStorage = ConcurrentMemoryStorage.<K, V>builder()
                    .setId(storageEndpoint.getStorageId())
                    .build();
            logger.info("Federated Storage {} is built with Concurrent Memory Storage ", storageEndpoint.getStorageId());
        }

        var backupMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var backupEndpoint = this.backupEndpointBuilder
                .setDefaultResolvingEndpointIdsSupplier(this.grid::getRemoteEndpointIds)
                .setMessageSerDe(backupMessageSerDe)
                .setProtocol(BackupStorage.PROTOCOL_NAME)
                .build();
        backupEndpoint.requestsDispatcher().subscribe(this.grid::send);
        this.storageEndpointBuiltListener.accept(backupEndpoint);
        var backups = new BackupStorageBuilder<K, V>()
                .withEndpoint(backupEndpoint)
                .build();

        var result = new FederatedStorage<K, V>(
                this.actualStorage,
                storageEndpoint,
                backups,
                this.mergeOperator,
                config
        );
        this.storageBuiltListener.accept(result);
        return result;

    }

}
