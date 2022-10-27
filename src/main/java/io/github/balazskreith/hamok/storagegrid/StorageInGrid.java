package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;
import io.reactivex.rxjava3.core.Observable;

interface StorageInGrid {

    String getIdentifier();
    void accept(Models.Message message);
    void close();
    StorageSyncResult executeSync();
    Observable<String> observableClosed();

    StorageEndpoint.Stats storageEndpointStats();
}
