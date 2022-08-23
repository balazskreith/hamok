package io.github.balazskreith.hamok.raccoons;

import java.util.UUID;

public record RaccoonConfig(
        UUID id,
        int electionTimeoutInMs,
        int followerMaxIdleInMs,
        int heartbeatInMs,
        int sendingHelloTimeoutInMs,
        int peerMaxIdleTimeInMs
) {
    public static RaccoonConfig create() {
        return new RaccoonConfig(
                UUID.randomUUID(), // the local peer id
                1000, // election timeout
                1000, // timeout for followers before starts an election
                300, // heartbeat
                1000, // hello message sending if auto discovery on
                1500 // the max idle time for a follower to be idle before the leader renders it inactive
        );

    }

    public RaccoonConfig copyAndSetId(UUID id) {
        return new RaccoonConfig(
                id,
                this.electionTimeoutInMs,
                this.followerMaxIdleInMs,
                this.heartbeatInMs,
                this.sendingHelloTimeoutInMs,
                this.peerMaxIdleTimeInMs
        );
    }

    public RaccoonConfig copyAndSetElectionTimeoutInMs(int electionTimeoutInMs) {
        return new RaccoonConfig(
                this.id,
                electionTimeoutInMs,
                this.followerMaxIdleInMs,
                this.heartbeatInMs,
                this.sendingHelloTimeoutInMs,
                this.peerMaxIdleTimeInMs
        );
    }

    public RaccoonConfig copyAndSetFollowerMaxIdleInMs(int followerMaxIdleInMs) {
        return new RaccoonConfig(
                this.id,
                this.electionTimeoutInMs,
                followerMaxIdleInMs,
                this.heartbeatInMs,
                this.sendingHelloTimeoutInMs,
                this.peerMaxIdleTimeInMs
        );
    }

    public RaccoonConfig copyAndSetHeartbeatInMs(int heartbeatInMs) {
        return new RaccoonConfig(
                this.id,
                this.electionTimeoutInMs,
                this.followerMaxIdleInMs,
                heartbeatInMs,
                this.sendingHelloTimeoutInMs,
                this.peerMaxIdleTimeInMs
        );
    }

    public RaccoonConfig copyAndSetSendingHelloTimeoutInMs(int sendingHelloTimeoutInMs) {
        return new RaccoonConfig(
                this.id,
                this.electionTimeoutInMs,
                this.followerMaxIdleInMs,
                this.heartbeatInMs,
                sendingHelloTimeoutInMs,
                this.peerMaxIdleTimeInMs
        );
    }

    public RaccoonConfig copyAndSetPeerMaxIdleTimeInMs(int peerMaxIdleTimeInMs) {
        return new RaccoonConfig(
                this.id,
                this.electionTimeoutInMs,
                this.followerMaxIdleInMs,
                this.heartbeatInMs,
                this.sendingHelloTimeoutInMs,
                peerMaxIdleTimeInMs
        );
    }
}
