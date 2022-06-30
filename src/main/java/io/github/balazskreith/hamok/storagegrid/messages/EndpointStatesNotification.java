package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.Set;
import java.util.UUID;

/**
 * Can only be created by the leader
 */
public record EndpointStatesNotification(
        UUID sourceEndpointId,
        Set<UUID> activeEndpointIds,
        Set<UUID> inactiveEndpointIds,
        UUID destinationEndpointId)
{

}
