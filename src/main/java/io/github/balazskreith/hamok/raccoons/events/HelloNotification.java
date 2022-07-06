package io.github.balazskreith.hamok.raccoons.events;

import java.util.UUID;

/**
 * All individual endpoint creates and regularly send id
 */
public record HelloNotification(UUID sourcePeerId, UUID raftLeaderId) {

}
