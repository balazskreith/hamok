package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.StorageEntry;

import java.util.Iterator;
import java.util.Set;

public interface DistributedStorage<K, V> extends Storage<K, V> {
    boolean localIsEmpty();
    int localSize();
    Set<K> localKeys();
//    void localClear();
    Iterator<StorageEntry<K, V>> localIterator();
}
