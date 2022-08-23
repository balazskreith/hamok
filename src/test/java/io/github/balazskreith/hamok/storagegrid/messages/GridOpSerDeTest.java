package io.github.balazskreith.hamok.storagegrid.messages;

import io.github.balazskreith.hamok.raccoons.events.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

class GridOpSerDeTest {

    private static GridOpSerDe gridOpSerDe;

    @BeforeAll
    static void setup() {
        gridOpSerDe = new GridOpSerDe();
    }

    @Test
    void endpointStatesNotification() {
        var expected = new EndpointStatesNotification(
                UUID.randomUUID(),
                Set.of(UUID.randomUUID()),
                1,
                2,
                3,
                UUID.randomUUID(),
                4
        );
        var message = gridOpSerDe.serializeEndpointStatesNotification(expected);
        var actual = gridOpSerDe.deserializeEndpointStatesNotification(message.build());

        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.activeEndpointIds().size(), actual.activeEndpointIds().size());
        Assertions.assertEquals(expected.numberOfLogs(), actual.numberOfLogs());
        Assertions.assertEquals(expected.leaderNextIndex(), actual.leaderNextIndex());
        Assertions.assertEquals(expected.commitIndex(), actual.commitIndex());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.term(), actual.term());
    }

    @Test
    void raftAppendRequestChunk() {
        var expected = new RaftAppendEntriesRequestChunk(
              UUID.randomUUID(),
                1,
                UUID.randomUUID(),
                2,
                3,
                null,
                4,
                5,
                6,
                true,
                UUID.randomUUID()
        );
        var message = gridOpSerDe.serializeRaftAppendRequestChunk(expected);
        var actual = gridOpSerDe.deserializeRaftAppendRequestChunk(message.build());

        Assertions.assertEquals(expected.peerId(), actual.peerId());
        Assertions.assertEquals(expected.term(), actual.term());
        Assertions.assertEquals(expected.leaderId(), actual.leaderId());
        Assertions.assertEquals(expected.prevLogIndex(), actual.prevLogIndex());
        Assertions.assertEquals(expected.prevLogTerm(), actual.prevLogTerm());
        Assertions.assertEquals(expected.entry(), actual.entry());
        Assertions.assertEquals(expected.leaderCommit(), actual.leaderCommit());
        Assertions.assertEquals(expected.leaderNextIndex(), actual.leaderNextIndex());
        Assertions.assertEquals(expected.sequence(), actual.sequence());
        Assertions.assertEquals(expected.lastMessage(), actual.lastMessage());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void raftAppendResponse() {
        var expected = new RaftAppendEntriesResponse(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                true,
                2,
                false
        );
        var message = gridOpSerDe.serializeRaftAppendResponse(expected);
        var actual = gridOpSerDe.deserializeRaftAppendResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.sourcePeerId(), actual.sourcePeerId());
        Assertions.assertEquals(expected.destinationPeerId(), actual.destinationPeerId());
        Assertions.assertEquals(expected.term(), actual.term());
        Assertions.assertEquals(expected.success(), actual.success());
        Assertions.assertEquals(expected.peerNextIndex(), actual.peerNextIndex());
        Assertions.assertEquals(expected.processed(), actual.processed());
    }


    @Test
    void raftVoteRequest() {
        var expected = new RaftVoteRequest(
                UUID.randomUUID(),
                1,
                UUID.randomUUID(),
                2,
                3
        );
        var message = gridOpSerDe.serializeRaftVoteRequest(expected);
        var actual = gridOpSerDe.deserializeRaftVoteRequest(message.build());

        Assertions.assertEquals(expected.peerId(), actual.peerId());
        Assertions.assertEquals(expected.term(), actual.term());
        Assertions.assertEquals(expected.candidateId(), actual.candidateId());
        Assertions.assertEquals(expected.lastLogIndex(), actual.lastLogIndex());
        Assertions.assertEquals(expected.lastLogTerm(), actual.lastLogTerm());
    }


    @Test
    void raftVoteResponse() {
        var expected = new RaftVoteResponse(
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                true
        );
        var message = gridOpSerDe.serializeRaftVoteResponse(expected);
        var actual = gridOpSerDe.deserializeRaftVoteResponse(message.build());

        Assertions.assertEquals(expected.destinationPeerId(), actual.destinationPeerId());
        Assertions.assertEquals(expected.sourcePeerId(), actual.sourcePeerId());
        Assertions.assertEquals(expected.term(), actual.term());
        Assertions.assertEquals(expected.voteGranted(), actual.voteGranted());
    }


    @Test
    void submitRequest() {
        var expected = new SubmitRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                null
        );

        var message = gridOpSerDe.serializeSubmitRequest(expected);
        var actual = gridOpSerDe.deserializeSubmitRequest(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.destinationId(), actual.destinationId());
        Assertions.assertEquals(expected.entry(), actual.entry());
    }


    @Test
    void helloNotification() {
        var expected = new HelloNotification(
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = gridOpSerDe.serializeHelloNotification(expected);
        var actual = gridOpSerDe.deserializeHelloNotification(message.build());

        Assertions.assertEquals(expected.sourcePeerId(), actual.sourcePeerId());
        Assertions.assertEquals(expected.raftLeaderId(), actual.raftLeaderId());
    }


    @Test
    void submitResponse() {
        var expected = new SubmitResponse(
                UUID.randomUUID(),
                UUID.randomUUID(),
                true,
                UUID.randomUUID()
        );
        var message = gridOpSerDe.serializeSubmitResponse(expected);
        var actual = gridOpSerDe.deserializeSubmitResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.leaderId(), actual.leaderId());
        Assertions.assertEquals(expected.success(), actual.success());
        Assertions.assertEquals(expected.leaderId(), actual.leaderId());
    }

    @Test
    void storageSyncRequest() {
        var expected = new StorageSyncRequest(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = gridOpSerDe.serializeStorageSyncRequest(expected);
        var actual = gridOpSerDe.deserializeStorageSyncRequest(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.leaderId(), actual.leaderId());
    }


    @Test
    void storageSyncResponse() {
        var expected = new StorageSyncResponse(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                2,
                3
        );
        var message = gridOpSerDe.serializeStorageSyncResponse(expected);
        var actual = gridOpSerDe.deserializeStorageSyncResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.destinationId(), actual.destinationId());
        Assertions.assertEquals(expected.leaderId(), actual.leaderId());
        Assertions.assertEquals(expected.numberOfLogs(), actual.numberOfLogs());
        Assertions.assertEquals(expected.commitIndex(), actual.commitIndex());
        Assertions.assertEquals(expected.lastApplied(), actual.lastApplied());
    }
}