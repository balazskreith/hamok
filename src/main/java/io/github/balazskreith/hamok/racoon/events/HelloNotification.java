package io.github.balazskreith.hamok.racoon.events;

import java.util.UUID;

/**
 * All individual endpoint creates and regularly send id
 */
public record HelloNotification(UUID sourcePeerId, UUID raftLeaderId) {

}
