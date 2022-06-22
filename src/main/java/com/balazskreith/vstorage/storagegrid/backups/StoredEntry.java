package com.balazskreith.vstorage.storagegrid.backups;

import java.util.UUID;

record StoredEntry<K, V>(K key, V value, UUID remoteEndpointId) {
}
