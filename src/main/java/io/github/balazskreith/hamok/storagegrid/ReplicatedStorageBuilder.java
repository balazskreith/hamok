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
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ReplicatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageBuilder.class);

    private final StorageEndpointBuilder<K, V> storageEndpointBuilder = new StorageEndpointBuilder<>();


    private Supplier<Codec<K, String>> keyCodecSupplier;
    private Supplier<Codec<V, String>> valueCodecSupplier;
    private Storage<K, V> actualStorage;
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<ReplicatedStorage<K, V>> storageBuiltListener = storage -> {};

    ReplicatedStorageBuilder() {

    }

    ReplicatedStorageBuilder<K, V> setStorageGrid(StorageGrid storageGrid) {
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

    ReplicatedStorageBuilder<K, V> setMapDepotProvider(Supplier<Depot<Map<K, V>>> depotProvider) {
        this.storageEndpointBuilder.setMapDepotProvider(depotProvider);
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setStorageId(String value) {
        this.storageEndpointBuilder.setStorageId(value);
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }


    public ReplicatedStorageBuilder<K, V> setKeyCodecSupplier(Supplier<Codec<K, String>> value) {
        this.keyCodecSupplier = value;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setValueCodecSupplier(Supplier<Codec<V, String>> value) {
        this.valueCodecSupplier = value;
        return this;
    }

    public ReplicatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueCodecSupplier, "Codec for values must be defined");
        Objects.requireNonNull(this.keyCodecSupplier, "Codec for keys must be defined");

        var actualMessageSerDe = new StorageOpSerDe<K, V>(this.keyCodecSupplier.get(), this.valueCodecSupplier.get());
        var storageEndpoint = this.storageEndpointBuilder
                .setMessageSerDe(actualMessageSerDe)
                .setProtocol(ReplicatedStorage.PROTOCOL_NAME)
                .build();
        this.storageEndpointBuiltListener.accept(storageEndpoint);
        if (this.actualStorage == null) {
            this.actualStorage = ConcurrentMemoryStorage.<K, V>builder()
                    .setId(storageEndpoint.getStorageId())
                    .build();
            logger.info("Separated Storage {} is built by using concurrent memory storage", storageEndpoint.getStorageId());
        }
        var result = new ReplicatedStorage<K, V>(this.actualStorage, storageEndpoint);
        this.storageBuiltListener.accept(result);
        return result;

    }

}
