package io.github.balazskreith.hamok.storagegrid;

public record StorageEndpointConfig(
        String protocol,
        boolean requestCanThrowTimeoutException
) {
}
