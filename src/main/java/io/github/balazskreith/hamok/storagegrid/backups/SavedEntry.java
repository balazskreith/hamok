package io.github.balazskreith.hamok.storagegrid.backups;

import java.util.UUID;

record SavedEntry<K>(K key, UUID remoteEndpointId) {
}
