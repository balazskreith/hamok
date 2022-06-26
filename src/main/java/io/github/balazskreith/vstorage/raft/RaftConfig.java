package io.github.balazskreith.vstorage.raft;

import java.util.UUID;

public record RaftConfig(
        int electionMinTimeoutInMs,
        int electionMaxRandomOffsetInMs,
        int heartbeatInMs,
        int applicationCommitIndexSyncTimeoutInMs,
        UUID id
) {

}
