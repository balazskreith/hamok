package io.github.balazskreith.hamok.raccoons.events;

import io.github.balazskreith.hamok.storagegrid.messages.Message;

import java.util.UUID;

public record RaftAppendEntriesRequestChunk(
        UUID peerId, // destination endpoint id
        int term,
        UUID leaderId, // source endpoint id
        int prevLogIndex,
        int prevLogTerm,
//        List<Message> entries,
        Message entry,
        int leaderCommit,
        int leaderNextIndex,
        int sequence,
        boolean lastMessage,
        UUID requestId
        )
{
        public RaftAppendEntriesResponse createResponse(boolean success, int peerNextIndex, boolean processed) {
                return new RaftAppendEntriesResponse(
                        this.peerId,
                        this.leaderId,
                        this.term,
                        success,
                        peerNextIndex,
                        processed
                );
        }

}
