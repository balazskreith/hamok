package io.github.balazskreith.hamok.raft;

import java.util.UUID;

public record RaftConfig(
        int electionMinTimeoutInMs,
        int electionMaxRandomOffsetInMs,
        int heartbeatInMs,
        int applicationCommitIndexSyncTimeoutInMs,
        UUID id
) {

}
