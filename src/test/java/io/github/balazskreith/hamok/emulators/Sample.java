package io.github.balazskreith.hamok.emulators;

import java.util.UUID;

public record Sample(String roomId, UUID clientId, UUID trackId, Double sentBytes) {
}
