package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.memorystorages.ConcurrentMemoryStorage;
import io.github.balazskreith.hamok.storagegrid.backups.BackupStorage;
import io.github.balazskreith.hamok.storagegrid.backups.DistributedBackups;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.storagegrid.messages.MessageType;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import io.reactivex.rxjava3.core.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class SeparatedStorageBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageBuilder.class);

    private Consumer<StorageInGrid> storageInGridListener = null;

    private Function<K, byte[]> keyEncoder;
    private Function<byte[], K> keyDecoder;
    private Function<V, byte[]> valueEncoder;
    private Function<byte[], V> valueDecoder;

    private BinaryOperator<V> mergeOp = null;
    private Storage<K, V> actualStorage;
    private StorageGrid grid = null;
    private String storageId = null;
    private int maxCollectedActualStorageEvents = 100;
    private int maxCollectedActualStorageTimeInMs = 100;
    private int iteratorBatchSize = 300;
    private int maxMessageKeys = 0;
    private int maxMessageValues = 0;
    private DistributedBackups distributedBackups = null;


    SeparatedStorageBuilder() {

    }

    SeparatedStorageBuilder<K, V> setStorageGrid(StorageGrid storageGrid) {
        this.grid = storageGrid;
        return this;
    }

    SeparatedStorageBuilder<K, V> onStorageInGridReady(Consumer<StorageInGrid> listener) {
        this.storageInGridListener = listener;
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
        return this;
    }

    public SeparatedStorageBuilder<K, V> setMaxMessageValues(int value) {
        this.maxMessageValues = value;
        return this;
    }

    public SeparatedStorageBuilder<K, V> setIteratorBatchSize(int value) {
        this.iteratorBatchSize = value;
        return this;
    }

    // make it not public yet, as storage also have a binary operator to merge the results
    SeparatedStorageBuilder<K, V> setMergeOperator(BinaryOperator<V> mergeOperator) {
        this.mergeOp = mergeOperator;
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

    public SeparatedStorageBuilder<K, V> setDistributedBackups(DistributedBackups distributedBackups) {
        this.distributedBackups = distributedBackups;
        return this;
    }

    private Function<Message, Iterator<Message>> createResponseMessageChunker() {
        if (this.maxMessageKeys < 1 && this.maxMessageValues < 1) {
            return ResponseMessageChunker.createSelfIteratorProvider();
        }
        return new ResponseMessageChunker(this.maxMessageKeys, this.maxMessageValues);
    }

    private Supplier<Depot<Map<K, V>>> createDepotProvider() {
        if (this.mergeOp == null) {
            return MapUtils::makeMapAssignerDepot;
        }
        return () -> MapUtils.makeMergedMapDepot(mergeOp);
    }


    public SeparatedStorage<K, V> build() {
        Objects.requireNonNull(this.valueEncoder, "Codec for values must be defined");
        Objects.requireNonNull(this.valueDecoder, "Codec for values must be defined");
        Objects.requireNonNull(this.keyEncoder, "Codec for keys must be defined");
        Objects.requireNonNull(this.valueDecoder, "Codec for keys must be defined");
        Objects.requireNonNull(this.storageInGridListener, "Separated Storage builder must have a callback for grid participation");
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
        var responseMessageChunker = this.createResponseMessageChunker();
        var depotProvider = this.createDepotProvider();
        var storageEndpoint = new StorageEndpoint<K, V>(
                this.grid,
                actualMessageSerDe,
                responseMessageChunker,
                depotProvider,
                SeparatedStorage.PROTOCOL_NAME
        ) {
            @Override
            protected String getStorageId() {
                return actualStorage.getId();
            }

            @Override
            protected Set<UUID> defaultResolvingEndpointIds(MessageType messageType) {
                return grid.endpoints().getRemoteEndpointIds();
            }

            @Override
            protected void sendNotification(Message message) {
                grid.send(message);
            }

            @Override
            protected void sendRequest(Message message) {
                grid.send(message);
            }

            @Override
            protected void sendResponse(Message message) {
                grid.send(message);
            }
        };
        BackupStorage<K, V> backupStorage = null;
        if (this.distributedBackups != null) {
            backupStorage = this.distributedBackups.createAdapter(
                    actualStorage.getId(),
                    keyEncoder,
                    keyDecoder,
                    valueEncoder,
                    valueDecoder
            );
        }
        var result = new SeparatedStorage<K, V>(
                this.actualStorage,
                storageEndpoint,
                backupStorage,
                config
        );
        this.storageInGridListener.accept(new StorageInGrid() {
            @Override
            public String getIdentifier() {
                return result.getId();
            }

            @Override
            public void accept(Message message) {
                storageEndpoint.receive(message);
            }

            @Override
            public void close() {
                storageEndpoint.close();
                try {
                    result.close();
                } catch (Exception e) {
                    logger.warn("Error occurred while closing storage", e);
                }
            }

            @Override
            public StorageSyncResult executeSync() {
                return result.executeSync();
            }

            @Override
            public Observable<String> observableClosed() {
                return result.events().closingStorage();
            }
        });

        return result;
    }
}
