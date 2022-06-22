package com.balazskreith.vstorage.raft.events;

import java.util.List;
import java.util.UUID;

public record RaftAppendEntriesRequest (
        UUID peerId, // destination endpoint id
        int term,
        UUID leaderId, // source endpoint id
        int prevLogIndex,
        int prevLogTerm,
        List<byte[]> entries,
        int leaderCommit,
        int leaderNextIndex
        )
{

        public RaftAppendEntriesResponse createResponse(boolean success, int peerNextIndex) {
                return new RaftAppendEntriesResponse(
                        this.peerId,
                        this.leaderId,
                        this.term,
                        success,
                        peerNextIndex
                );
        }
}
