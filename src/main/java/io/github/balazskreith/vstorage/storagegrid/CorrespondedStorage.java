package io.github.balazskreith.vstorage.storagegrid;

import io.github.balazskreith.vstorage.Storage;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

record CorrespondedStorage<K, V>(Storage<K, V> storage, List<StorageEndpoint> endpoints) {

    private static final CorrespondedStorage EMPTY = new CorrespondedStorage(null, Collections.emptyList());

    static<U, R> CorrespondedStorage<U, R> createEmpty() {
        return (CorrespondedStorage<U, R>) EMPTY;
    }

    CorrespondedStorage<K, V> addEndpoint(StorageEndpoint endpoint) {
        List<StorageEndpoint> updatedList = Stream.concat(this.endpoints.stream(), List.of(endpoint).stream()).collect(Collectors.toList());
        return new CorrespondedStorage<K, V>(
                this.storage,
                updatedList
        );
    }

    CorrespondedStorage<K, V> setStorage(Storage<K, V> storage) {
        return new CorrespondedStorage<K, V>(
                storage,
                this.endpoints
        );
    }

}
