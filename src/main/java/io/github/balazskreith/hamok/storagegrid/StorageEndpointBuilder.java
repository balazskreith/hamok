package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Depot;
import io.github.balazskreith.hamok.storagegrid.messages.StorageOpSerDe;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

public class StorageEndpointBuilder<U, R> {
    private volatile boolean built = false;
    private final StorageEndpoint<U, R> result = new StorageEndpoint<>();

    StorageEndpointBuilder() {

    }

    StorageEndpointBuilder<U, R> setMessageSerDe(StorageOpSerDe<U, R> messageSerDe) {
        this.result.messageSerDe = messageSerDe;
        return this;
    }

    StorageEndpointBuilder<U, R> setStorageGrid(StorageGrid grid) {
        this.result.grid = grid;
        return this;
    }

    StorageEndpointBuilder<U, R> setStorageId(String value) {
        this.result.storageId = value;
        return this;
    }

    StorageEndpointBuilder<U, R> setDefaultResolvingEndpointIdsSupplier(Supplier<Set<UUID>> defaultResolvingEndpointIdsSupplier) {
        this.result.defaultResolvingEndpointIds = defaultResolvingEndpointIdsSupplier;
        return this;
    }

    StorageEndpointBuilder<U, R> setProtocol(String value) {
        this.result.protocol = value;
        return this;
    }

    StorageEndpointBuilder<U, R> setMapDepotProvider(Supplier<Depot<Map<U, R>>> depotProvider) {
        this.result.depotProvider = depotProvider;
        return this;
    }

    public StorageEndpoint<U, R> build() {
        if (this.built) throw new IllegalStateException("Cannot build twice");
        Objects.requireNonNull(this.result.storageId, "StorageId cannot be null");
        Objects.requireNonNull(this.result.grid, "storageGrid must be defined");
        Objects.requireNonNull(this.result.depotProvider, "Depot provider must be given");
        this.result.init();
        try {
            return this.result;
        } finally {
            this.built = true;
        }
    }
}
