package io.github.balazskreith.hamok.storagegrid.backups;

import java.util.UUID;

record StoredEntry<K, V>(K key, V value, UUID remoteEndpointId) {
}
