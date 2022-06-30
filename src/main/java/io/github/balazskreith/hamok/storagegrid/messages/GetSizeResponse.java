package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.UUID;

public record GetSizeResponse<K>(UUID requestId, int size, UUID destinationEndpointId) {

}
