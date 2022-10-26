package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Observable;

interface StorageInGrid {

    String getIdentifier();
    void accept(Message message);
    void close();
    StorageSyncResult executeSync();
    Observable<String> observableClosed();
}
