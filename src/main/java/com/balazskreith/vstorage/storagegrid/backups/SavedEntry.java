package com.balazskreith.vstorage.storagegrid.backups;

import java.util.UUID;

record SavedEntry<K>(K key, UUID remoteEndpointId) {
}
