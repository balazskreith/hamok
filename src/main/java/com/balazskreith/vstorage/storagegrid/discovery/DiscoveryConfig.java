package com.balazskreith.vstorage.storagegrid.discovery;

import java.util.UUID;

public record DiscoveryConfig(
        UUID localEndpointId,
        int maxIdleRemoteEndpointInMs
) {
}
