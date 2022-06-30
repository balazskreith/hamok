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

    private Supplier<Codec<K, String>> keyCodecSupplier;
    private Supplier<Codec<V, String>> valueCodecSupplier;
    private Supplier<BinaryOperator<V>> mergeOpProvider;
    private Storage<K, V> actualStorage;

    FederatedStorageBuilder() {

    }

    FederatedStorageBuilder<K, V> setStorageGrid(StorageGrid grid) {
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

    public FederatedStorageBuilder<K, V> setMergeOperator(Supplier<BinaryOperator<V>> mergeOpProvider) {
        Supplier<Depot<Map<K, V>>> mergedMapDepotProvider = () -> MapUtils.makeMergedMapDepot(mergeOpProvider.get());
        this.storageEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.backupEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.mergeOpProvider = mergeOpProvider;
        return this;
    }


    public FederatedStorageBuilder<K, V> setStorageId(String value) {
        this.backupEndpointBuilder.setStorageId(value);
        this.storageEndpointBuilder.setStorageId(value);
        return this;
    }

    public FederatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }


    public FederatedStorageBuilder<K, V> setKeyCodecSupplier(Supplier<Codec<K, String>> value) {
        this.keyCodecSupplier = value;
        return this;
    }

    public FederatedStorageBuilder<K, V> setValueCodecSupplier(Supplier<Codec<V, String>> value) {
        this.valueCodecSupplier = value;
        return this;
    }

    public FederatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueCodecSupplier, "Codec for values must be defined");
        Objects.requireNonNull(this.keyCodecSupplier, "Codec for keys must be defined");
        Objects.requireNonNull(this.mergeOpProvider, "Cannot build without merge operator");

        var actualMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var storageEndpoint = this.storageEndpointBuilder
                .setMessageSerDe(actualMessageSerDe)
                .setProtocol(FederatedStorage.PROTOCOL_NAME)
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

        var result = new FederatedStorage<K, V>(this.actualStorage, storageEndpoint, backups, mergeOpProvider.get());
        this.storageBuiltListener.accept(result);
        return result;

    }

}
