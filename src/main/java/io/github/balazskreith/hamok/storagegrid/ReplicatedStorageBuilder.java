package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.hamok.storagegrid.messages.MessageType;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class ReplicatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageBuilder.class);

    private final StorageEndpointBuilder<K, V> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<ReplicatedStorage<K, V>> storageBuiltListener = storage -> {};
    private StorageGrid grid;

    private Function<K, byte[]> keyEncoder;
    private Function<byte[], K> keyDecoder;
    private Function<V, byte[]> valueEncoder;
    private Function<byte[], V> valueDecoder;

    private Storage<K, V> actualStorage;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;
    private int maxMessageKeys = 0;
    private int maxMessageValues = 0;

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

    public ReplicatedStorageBuilder<K, V> setMaxMessageKeys(int value) {
        this.maxMessageKeys = value;
        this.storageEndpointBuilder.setMaxMessageKeys(value);
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setMaxMessageValues(int value) {
        this.maxMessageValues = value;
        this.storageEndpointBuilder.setMaxMessageValues(value);
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

    public ReplicatedStorageBuilder<K, V> setStorageId(String value) {
        this.storageId = value;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setStorage(Storage<K, V> actualStorage) {
        this.actualStorage = actualStorage;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setKeyCodec(Function<K, byte[]> encoder, Function<byte[], K> decoder) {
        this.keyEncoder = encoder;
        this.keyDecoder = decoder;
        return this;
    }

    public ReplicatedStorageBuilder<K, V> setValueCodec(Function<V, byte[]> encoder, Function<byte[], V> decoder) {
        this.valueEncoder = encoder;
        this.valueDecoder = decoder;
        return this;
    }


    ReplicatedStorageBuilder<K, V> setMapDepotProvider(Supplier<Depot<Map<K, V>>> depotProvider) {
        this.storageEndpointBuilder.setMapDepotProvider(depotProvider);
        return this;
    }


    public ReplicatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueEncoder, "Codec for values must be defined");
        Objects.requireNonNull(this.valueDecoder, "Codec for values must be defined");
        Objects.requireNonNull(this.keyEncoder, "Codec for keys must be defined");
        Objects.requireNonNull(this.valueDecoder, "Codec for keys must be defined");

        if (this.actualStorage == null) {
            Objects.requireNonNull(this.storageId, "Cannot build without storage Id");
            this.actualStorage = ConcurrentMemoryStorage.<K, V>builder()
                    .setId(this.storageId)
                    .build();
            logger.info("Replicated Storage {} is built with Concurrent Memory Storage ", this.storageId);
        } else {
            this.storageId = this.actualStorage.getId();
        }

        var config = new ReplicatedStorageConfig(
                this.storageId,
                this.maxCollectedActualStorageEvents,
                this.maxCollectedActualStorageTimeInMs,
                this.maxMessageKeys,
                this.maxMessageValues
        );

        var actualMessageSerDe = new StorageOpSerDe<K, V>(
                this.keyEncoder,
                this.keyDecoder,
                this.valueEncoder,
                this.valueDecoder
        );
        var localEndpointSet = Set.of(this.grid.getLocalEndpointId());
        var storageEndpoint = this.storageEndpointBuilder
                .setStorageId(this.storageId)
                .setMessageSerDe(actualMessageSerDe)
                .setDefaultResolvingEndpointIdsSupplier(() -> localEndpointSet)
                .setProtocol(ReplicatedStorage.PROTOCOL_NAME)
                .build();
        storageEndpoint.requestsDispatcher().subscribe(message -> {
            var type = MessageType.valueOf(message.type);
            switch (type) {
                case GET_ENTRIES_REQUEST,
                        GET_SIZE_REQUEST,
                        GET_KEYS_REQUEST -> {
                    this.grid.send(message);
                }
                default -> {
                    this.grid.submit(message);
                }
            }
        });
        this.storageEndpointBuiltListener.accept(storageEndpoint);

        var result = new ReplicatedStorage<K, V>(
                this.actualStorage,
                storageEndpoint,
                config
        );
        this.storageBuiltListener.accept(result);
        return result;

    }
}
