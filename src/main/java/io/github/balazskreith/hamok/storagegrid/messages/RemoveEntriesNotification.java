package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Map;
import java.util.UUID;

public record RemoveEntriesNotification<K, V>(Map<K, V> entries, UUID sourceEndpointId) {

}
