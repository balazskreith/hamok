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
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FederatedMultiStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(FederatedMultiStorageBuilder.class);

    private final StorageEndpointBuilder<K, Set<V>> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private final StorageEndpointBuilder<K, Set<V>> backupEndpointBuilder = new StorageEndpointBuilder<>();
    private Consumer<StorageEndpoint<K, Set<V>>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<FederatedMultiStorage<K, V>> storageBuiltListener = storage -> {};
    private StorageGrid grid;

    private Supplier<Codec<K, String>> keyCodecSupplier;
    private Supplier<Codec<Set<V>, String>> valueCodecSupplier;
    private BinaryOperator<Set<V>> mergeOperator;
    private BiFunction<V, V, Boolean> equalsOperator;
    private Storage<K, Set<V>> actualStorage;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;
    private int iteratorBatchSize = 300;

    FederatedMultiStorageBuilder() {
        this.mergeOperator = (set1, set2) -> Stream
                .concat(set1.stream(), set2.stream())
                .collect(Collectors.toSet());
        this.equalsOperator = (v1, v2) -> {
            if (v1 == null && v2 == null) return true;
            if (v1 == null || v2 == null) return false;
            return v1.equals(v2);
        };
        Supplier<Depot<Map<K, Set<V>>>> mergedMapDepotProvider = () -> MapUtils.makeMergedMapDepot(this.mergeOperator);
        this.storageEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.backupEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
    }

    FederatedMultiStorageBuilder<K, V> setStorageGrid(StorageGrid grid) {
        this.grid = grid;
        this.storageEndpointBuilder.setStorageGrid(grid);
        this.backupEndpointBuilder.setStorageGrid(grid);
        return this;
    }

    FederatedMultiStorageBuilder<K, V> onEndpointBuilt(Consumer<StorageEndpoint<K, Set<V>>> listener) {
        this.storageEndpointBuiltListener = listener;
        return this;
    }

    FederatedMultiStorageBuilder<K, V> onStorageBuilt(Consumer<FederatedMultiStorage<K, V>> listener) {
        this.storageBuiltListener = listener;
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setMergeOperator(Supplier<BinaryOperator<Set<V>>> mergeOpProvider) {
        Supplier<Depot<Map<K, Set<V>>>> mergedMapDepotProvider = () -> MapUtils.makeMergedMapDepot(mergeOpProvider.get());
        this.storageEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.backupEndpointBuilder.setMapDepotProvider(mergedMapDepotProvider);
        this.mergeOperator = mergeOpProvider.get();
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setEqualsOperator(BiFunction<V, V, Boolean> equalsOperator) {
        this.equalsOperator = equalsOperator;
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setIteratorBatchSize(int value) {
        this.iteratorBatchSize = value;
        return this;
    }


    public FederatedMultiStorageBuilder<K, V> setStorageId(String value) {
        this.storageId = value;
        this.backupEndpointBuilder.setStorageId(value);
        this.storageEndpointBuilder.setStorageId(value);
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setMaxCollectedStorageEvents(int value) {
        this.maxCollectedActualStorageEvents = value;
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setMaxCollectedStorageTimeInMs(int value) {
        this.maxCollectedActualStorageTimeInMs = value;
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setStorage(Storage<K, Set<V>> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setKeyCodecSupplier(Supplier<Codec<K, String>> value) {
        this.keyCodecSupplier = value;
        return this;
    }

    public FederatedMultiStorageBuilder<K, V> setValueCodecSupplier(Supplier<Codec<Set<V>, String>> value) {
        this.valueCodecSupplier = value;
        return this;
    }

    public FederatedMultiStorage<K, V> build() {
        Objects.requireNonNull(this.valueCodecSupplier, "Codec for values must be defined");
        Objects.requireNonNull(this.keyCodecSupplier, "Codec for keys must be defined");
        Objects.requireNonNull(this.mergeOperator, "Cannot build without merge operator");
        Objects.requireNonNull(this.equalsOperator, "Cannot build without equals operator");
        Objects.requireNonNull(this.storageId, "Cannot build without storage Id");
        var config = new FederatedMultiStorageConfig(
                this.storageId,
                this.maxCollectedActualStorageEvents,
                this.maxCollectedActualStorageTimeInMs,
                this.iteratorBatchSize
        );

        var actualMessageSerDe = new StorageOpSerDe<K, Set<V>>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var storageEndpoint = this.storageEndpointBuilder
                .setDefaultResolvingEndpointIdsSupplier(this.grid::getRemoteEndpointIds)
                .setMessageSerDe(actualMessageSerDe)
                .setProtocol(FederatedMultiStorage.PROTOCOL_NAME)
                .build();
        storageEndpoint.requestsDispatcher().subscribe(this.grid::send);
        this.storageEndpointBuiltListener.accept(storageEndpoint);
        if (this.actualStorage == null) {
            this.actualStorage = ConcurrentMemoryStorage.<K, Set<V>>builder()
                    .setId(storageEndpoint.getStorageId())
                    .build();
            logger.info("Federated Storage {} is built with Concurrent Memory Storage ", storageEndpoint.getStorageId());
        }

        var backupMessageSerDe = new StorageOpSerDe<K, Set<V>>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var backupEndpoint = this.backupEndpointBuilder
                .setDefaultResolvingEndpointIdsSupplier(this.grid::getRemoteEndpointIds)
                .setMessageSerDe(backupMessageSerDe)
                .setProtocol(BackupStorage.PROTOCOL_NAME)
                .build();
        backupEndpoint.requestsDispatcher().subscribe(this.grid::send);
        this.storageEndpointBuiltListener.accept(backupEndpoint);
        var backups = new BackupStorageBuilder<K, Set<V>>()
                .withEndpoint(backupEndpoint)
                .build();

        var result = new FederatedMultiStorage<K, V>(
                this.actualStorage,
                storageEndpoint,
                backups,
                this.mergeOperator,
                this.equalsOperator,
                config
        );
        this.storageBuiltListener.accept(result);
        return result;

    }

}
