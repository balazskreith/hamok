package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Set;
import java.util.UUID;

public record DeleteEntriesNotification<K>(UUID sourceEndpointId, Set<K> keys, UUID destinationEndpointId) {
}
