package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorageBuilder;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class SeparatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageBuilder.class);

    private final StorageEndpointBuilder<K, V> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private final StorageEndpointBuilder<K, V> backupEndpointBuilder = new StorageEndpointBuilder<>();
    private Consumer<StorageEndpoint<K, V>> storageEndpointBuiltListener = endpoint -> {};
    private Consumer<SeparatedStorage<K, V>> storageBuiltListener = storage -> {};

    private Function<K, byte[]> keyEncoder;
    private Function<byte[], K> keyDecoder;
    private Function<V, byte[]> valueEncoder;
    private Function<byte[], V> valueDecoder;

    private Storage<K, V> actualStorage;
    private StorageGrid grid = null;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;
    private int iteratorBatchSize = 300;
    private int maxMessageKeys = 0;
    private int maxMessageValues = 0;


    SeparatedStorageBuilder() {

    }

    SeparatedStorageBuilder<K, V> setStorageGrid(StorageGrid storageGrid) {
        this.grid = storageGrid;
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

    public SeparatedStorageBuilder<K, V> setMaxMessageKeys(int value) {
        this.maxMessageKeys = value;
        this.storageEndpointBuilder.setMaxMessageKeys(value);
        return this;
    }

    public SeparatedStorageBuilder<K, V> setMaxMessageValues(int value) {
        this.maxMessageValues = value;
        this.storageEndpointBuilder.setMaxMessageValues(value);
        return this;
    }

    public SeparatedStorageBuilder<K, V> setIteratorBatchSize(int value) {
        this.iteratorBatchSize = value;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setKeyCodec(Function<K, byte[]> encoder, Function<byte[], K> decoder) {
        this.keyEncoder = encoder;
        this.keyDecoder = decoder;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setValueCodec(Function<V, byte[]> encoder, Function<byte[], V> decoder) {
        this.valueEncoder = encoder;
        this.valueDecoder = decoder;
        return this;
    }


    public SeparatedStorage<K, V> build() {
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
        var config = new SeparatedStorageConfig(
                this.storageId,
                this.maxCollectedActualStorageEvents,
                this.maxCollectedActualStorageTimeInMs,
                this.iteratorBatchSize,
                this.maxMessageKeys,
                this.maxMessageValues
        );

        var actualMessageSerDe = new StorageOpSerDe<K, V>(
                this.keyEncoder,
                this.keyDecoder,
                this.valueEncoder,
                this.valueDecoder
        );
        var storageEndpoint = this.storageEndpointBuilder
                .setStorageId(this.storageId)
                .setDefaultResolvingEndpointIdsSupplier(this.grid::getRemoteEndpointIds)
                .setMessageSerDe(actualMessageSerDe)
                .setProtocol(SeparatedStorage.PROTOCOL_NAME)
                .build();
        storageEndpoint.requestsDispatcher().subscribe(this.grid::send);
        this.storageEndpointBuiltListener.accept(storageEndpoint);

        var backupMessageSerDe = new StorageOpSerDe<K, V>(
                this.keyEncoder,
                this.keyDecoder,
                this.valueEncoder,
                this.valueDecoder
        );
        var backupEndpoint = this.backupEndpointBuilder
                .setStorageId(this.storageId)
                .setDefaultResolvingEndpointIdsSupplier(this.grid::getRemoteEndpointIds)
                .setMessageSerDe(backupMessageSerDe)
                .setProtocol(BackupStorage.PROTOCOL_NAME)
                .build();
        backupEndpoint.requestsDispatcher().subscribe(this.grid::send);
        this.storageEndpointBuiltListener.accept(backupEndpoint);
        var backups = new BackupStorageBuilder<K, V>()
                .withEndpoint(backupEndpoint)
                .build();

        var result = new SeparatedStorage<K, V>(this.actualStorage, storageEndpoint, backups, config);
        this.storageBuiltListener.accept(result);
        return result;
    }
}
