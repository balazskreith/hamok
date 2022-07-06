package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ReplicatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageBuilder.class);

    private final StorageEndpointBuilder<K, V> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<ReplicatedStorage<K, V>> storageBuiltListener = storage -> {};
    private StorageGrid grid;

    private Supplier<Codec<K, byte[]>> keyCodecSupplier;
    private Supplier<Codec<V, byte[]>> valueCodecSupplier;
    private Storage<K, V> actualStorage;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;
    private int maxMessageKeys = 10000;
    private int maxMessageValues = 1000;

    ReplicatedStorageBuilder() {

    }

    ReplicatedStorageBuilder<K, V> setStorageGrid(StorageGrid storageGrid) {
        this.grid = storageGrid;
        this.storageEndpointBuilder.setStorageGrid(storageGrid);
        return this;
    }

    ReplicatedStorageBuilder<K, V> onEndpointBuilt(Consumer<StorageEndpoint<K, V>> listener) {
        this.storageEndpointBuiltListener = listener;
        return this;
    }

    ReplicatedStorageBuilder<K, V> onStorageBuilt(Consumer<ReplicatedStorage<K, V>> listener) {
        this.storageBuiltListener = listener;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setMaxCollectedStorageEvents(int value) {
        this.maxCollectedActualStorageEvents = value;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setMaxCollectedStorageTimeInMs(int value) {
        this.maxCollectedActualStorageTimeInMs = value;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setMaxMessageKeys(int value) {
        this.maxMessageKeys = value;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setMaxMessageValues(int value) {
        this.maxMessageValues = value;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setStorageId(String value) {
        this.storageId = value;
        this.storageEndpointBuilder.setStorageId(value);
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setKeyCodecSupplier(Supplier<Codec<K, byte[]>> value) {
        this.keyCodecSupplier = value;
        return this;
    }

    ReplicatedStorageBuilder<K, V> setMapDepotProvider(Supplier<Depot<Map<K, V>>> depotProvider) {
        this.storageEndpointBuilder.setMapDepotProvider(depotProvider);
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setValueCodecSupplier(Supplier<Codec<V, byte[]>> value) {
        this.valueCodecSupplier = value;
        return this;
    }


    public ReplicatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueCodecSupplier, "Codec for values must be defined");
        Objects.requireNonNull(this.keyCodecSupplier, "Codec for keys must be defined");
        Objects.requireNonNull(this.storageId, "Cannot build without storage Id");
        var config = new ReplicatedStorageConfig(
                this.storageId,
                this.maxCollectedActualStorageEvents,
                this.maxCollectedActualStorageTimeInMs,
                this.maxMessageKeys,
                this.maxMessageValues
        );

        var actualMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var localEndpointSet = Set.of(this.grid.getLocalEndpointId());
        var storageEndpoint = this.storageEndpointBuilder
                .setMessageSerDe(actualMessageSerDe)
                .setDefaultResolvingEndpointIdsSupplier(() -> localEndpointSet)
                .setProtocol(ReplicatedStorage.PROTOCOL_NAME)
                .build();
        storageEndpoint.requestsDispatcher().subscribe(this.grid::submit);
        this.storageEndpointBuiltListener.accept(storageEndpoint);
        if (this.actualStorage == null) {
            this.actualStorage = ConcurrentMemoryStorage.<K, V>builder()
                    .setId(storageEndpoint.getStorageId())
                    .build();
            logger.info("Replicated Storage {} is built with Concurrent Memory Storage ", storageEndpoint.getStorageId());
        }

        var result = new ReplicatedStorage<K, V>(this.actualStorage, storageEndpoint, config);
        this.storageBuiltListener.accept(result);
        return result;

    }
}
