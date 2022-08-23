package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PropagatedCollectionsBuilder<K, V, T extends Collection<V>> {
    private static final Logger logger = LoggerFactory.getLogger(PropagatedCollectionsBuilder.class);


    private final StorageEndpointBuilder<K, T> storageEndpointBuilder = new StorageEndpointBuilder<>();
    private Consumer<PropagatedCollections<K, V, T>> propagatedCollectionsListener = propagatedCollections -> {};
    private Consumer<StorageEndpoint<K, T>> storageEndpointBuiltListener = endpoint -> {};
    private StorageGrid grid;

    private Function<K, byte[]> keyEncoder;
    private Function<byte[], K> keyDecoder;
    private Function<T, byte[]> valueEncoder;
    private Function<byte[], T> valueDecoder;
    private Supplier<T> newCollectionSupplier;
    private String gridEndpointId;

    PropagatedCollectionsBuilder() {

    }

    PropagatedCollectionsBuilder<K, V, T> setStorageGrid(StorageGrid storageGrid) {
        this.grid = storageGrid;
        this.storageEndpointBuilder.setStorageGrid(storageGrid);
        return this;
    }

    PropagatedCollectionsBuilder<K, V, T> onEndpointBuilt(Consumer<StorageEndpoint<K, T>> listener) {
        this.storageEndpointBuiltListener = listener;
        return this;
    }

    PropagatedCollectionsBuilder<K, V, T> onPropagatedCollectionsBuilt(Consumer<PropagatedCollections<K, V, T>> listener) {
        this.propagatedCollectionsListener = listener;
        return this;
    }

    public PropagatedCollectionsBuilder<K, V, T> setGridEndpointId(String endpointId) {
        this.gridEndpointId = endpointId;
        return this;
    }

    public PropagatedCollectionsBuilder<K, V, T> setNewCollectionSupplier(Supplier<T> newCollectionSupplier) {
        this.newCollectionSupplier = newCollectionSupplier;
        return this;
    }

    public PropagatedCollectionsBuilder<K, V, T> setKeyCodec(Function<K, byte[]> encoder, Function<byte[], K> decoder) {
        this.keyEncoder = encoder;
        this.keyDecoder = decoder;
        return this;
    }

    public PropagatedCollectionsBuilder<K, V, T> setCollectionCodec(Function<T, byte[]> encoder, Function<byte[], T> decoder) {
        this.valueEncoder = encoder;
        this.valueDecoder = decoder;
        return this;
    }

    public PropagatedCollectionsBuilder<K, V, T> setMapDepotProvider(Supplier<Depot<Map<K, T>>> depotProvider) {
        this.storageEndpointBuilder.setMapDepotProvider(depotProvider);
        return this;
    }

    public PropagatedCollections<K, V, T> build() {
        Objects.requireNonNull(this.valueEncoder, "Codec for values must be defined");
        Objects.requireNonNull(this.valueDecoder, "Codec for values must be defined");
        Objects.requireNonNull(this.keyEncoder, "Codec for keys must be defined");
        Objects.requireNonNull(this.gridEndpointId, "GridEndpointId is required to identify collections accross the grid");
        Objects.requireNonNull(this.newCollectionSupplier, "newCollectionsSupplier is required");

        var config = new PropagatedCollectionsConfig(
        );

        var actualMessageSerDe = new StorageOpSerDe<K, T>(
                this.keyEncoder,
                this.keyDecoder,
                this.valueEncoder,
                this.valueDecoder
        );
        var localEndpointSet = Set.of(this.grid.getLocalEndpointId());
        var storageEndpoint = this.storageEndpointBuilder
                .setStorageId(this.gridEndpointId)
                .setMessageSerDe(actualMessageSerDe)
                .setDefaultResolvingEndpointIdsSupplier(() -> localEndpointSet)
                .setProtocol(PropagatedCollections.PROTOCOL_NAME)
                .build();
        storageEndpoint.requestsDispatcher().subscribe(this.grid::submit);
        this.storageEndpointBuiltListener.accept(storageEndpoint);

        var result = new PropagatedCollections<K, V, T>(storageEndpoint, this.newCollectionSupplier,  config);
        this.propagatedCollectionsListener.accept(result);
        return result;

    }
}
