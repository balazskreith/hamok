package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Set;
import java.util.UUID;

public record GetKeysResponse<K>(UUID requestId, Set<K> keys, UUID destinationEndpointId) {

}
