package io.github.balazskreith.hamok.storagegrid.discovery;

import java.util.UUID;

public record DiscoveryConfig(
        UUID localEndpointId,
        int maxIdleRemoteEndpointInMs,
        int heartbeatInMs,
        int helloNotificationPeriodInMs,
        int endpointStateNotificationPeriodInMs
) {
}
