package io.github.balazskreith.hamok.emulators;

import java.util.UUID;

public record OutboundTrack(UUID trackId, UUID clientId, Double sentBytes) {
}
