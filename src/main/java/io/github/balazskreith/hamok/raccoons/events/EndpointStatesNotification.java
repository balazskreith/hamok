package io.github.balazskreith.hamok.raccoons.events;

import java.util.Set;
import java.util.UUID;

/**
 * Can only be created by the leader
 */
public record EndpointStatesNotification(
        UUID sourceEndpointId,
        Set<UUID> activeEndpointIds,
        Integer numberOfLogs,
        Integer leaderNextIndex,
        Integer commitIndex,
        UUID destinationEndpointId,
        Integer term
) {

}
