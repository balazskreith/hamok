package io.github.balazskreith.hamok.racoon;

import java.util.UUID;

public record RacoonConfig(
        UUID id,
        int electionTimeoutInMs,
        int followerMaxIdleInMs,
        int heartbeatInMs,
        int sendingHelloTimeoutInMs,
        int peerMaxIdleTimeInMs,
        int applicationCommitIndexSyncTimeoutInMs,
        boolean autoDiscovery
) {

}
