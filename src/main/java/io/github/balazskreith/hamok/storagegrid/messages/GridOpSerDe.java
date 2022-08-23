package io.github.balazskreith.hamok.storagegrid.messages;


import io.github.balazskreith.hamok.raccoons.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class GridOpSerDe {

    private static final Logger logger = LoggerFactory.getLogger(GridOpSerDe.class);

//    private final Base64.Encoder base64Encoder = Base64.getEncoder();
//    private final Base64.Decoder base64Decoder = Base64.getDecoder();

    public Message serializeEndpointStatesNotification(EndpointStatesNotification notification) {
        var result = new Message();
        result.type = MessageType.ENDPOINT_STATES_NOTIFICATION.name();
        result.sourceId = notification.sourceEndpointId();
        result.destinationId = notification.destinationEndpointId();
        result.activeEndpointIds = notification.activeEndpointIds().stream().collect(Collectors.toList());
        result.raftCommitIndex = notification.commitIndex();
        result.raftLeaderNextIndex = notification.leaderNextIndex();
        result.storageSize = notification.numberOfLogs();
        result.raftTerm = notification.term();
        return result;
    }

    public EndpointStatesNotification deserializeEndpointStatesNotification(Message message) {
        var activeEndpointIds = message.activeEndpointIds.stream().collect(Collectors.toSet());
        return new EndpointStatesNotification(
                message.sourceId,
                activeEndpointIds,
                message.storageSize,
                message.raftLeaderNextIndex,
                message.raftCommitIndex,
                message.destinationId,
                message.raftTerm
        );
    }

    public Message serializeRaftAppendRequestChunk(RaftAppendEntriesRequestChunk request) {
        var result = new Message();
        result.type = MessageType.RAFT_APPEND_ENTRIES_REQUEST_CHUNK.name();
        result.destinationId = request.peerId();
        result.raftLeaderId = request.leaderId();
        result.raftCommitIndex = request.leaderCommit();
        result.raftLeaderNextIndex = request.leaderNextIndex();
        result.raftPrevLogTerm = request.prevLogTerm();
        result.raftPrevLogIndex = request.prevLogIndex();
        result.raftTerm = request.term();
        result.embeddedMessages = request.entry() == null ? Collections.emptyList() : List.of(request.entry());
        result.requestId = request.requestId();
        result.sequence = request.sequence();
        result.lastMessage = request.lastMessage();
        return result;
    }

    public RaftAppendEntriesRequestChunk deserializeRaftAppendRequestChunk(Message message) {
        Message entry = null;
        if (message.embeddedMessages != null && 0 < message.embeddedMessages.size()) {
            entry = message.embeddedMessages.get(0);
            if (1 < message.embeddedMessages.size()) {
                logger.warn("More than one message received for RaftAppendRequestChunk. Only the first one will be processed");
            }
        }
        return new RaftAppendEntriesRequestChunk(
                message.destinationId,
                message.raftTerm,
                message.raftLeaderId,
                message.raftPrevLogIndex,
                message.raftPrevLogTerm,
                entry,
                message.raftCommitIndex,
                message.raftLeaderNextIndex,
                message.sequence,
                message.lastMessage,
                message.requestId
        );
    }

    public Message serializeRaftAppendResponse(RaftAppendEntriesResponse response) {
        var result = new Message();
        result.type = MessageType.RAFT_APPEND_ENTRIES_RESPONSE.name();
        result.success = response.success();
        result.requestId = response.requestId();
        result.raftTerm = response.term();
        result.destinationId = response.destinationPeerId();
        result.raftPeerNextIndex = response.peerNextIndex();
        result.lastMessage = response.processed();
        return result;
    }

    public RaftAppendEntriesResponse deserializeRaftAppendResponse(Message message) {
        return new RaftAppendEntriesResponse(
                message.sourceId,
                message.destinationId,
                message.requestId,
                message.raftTerm,
                message.success,
                message.raftPeerNextIndex,
                message.lastMessage
        );
    }

    public Message serializeRaftVoteRequest(RaftVoteRequest request) {
        var result = new Message();
        result.type = MessageType.RAFT_VOTE_REQUEST.name();
        result.destinationId = request.peerId();
        result.raftTerm = request.term();
        result.raftCandidateId = request.candidateId();
        result.raftPrevLogIndex = request.lastLogIndex();
        result.raftPrevLogTerm = request.lastLogTerm();
        return result;
    }

    public RaftVoteRequest deserializeRaftVoteRequest(Message message) {
        return new RaftVoteRequest(
                message.destinationId,
                message.raftTerm,
                message.raftCandidateId,
                message.raftPrevLogIndex,
                message.raftPrevLogTerm
        );
    }

    public Message serializeRaftVoteResponse(RaftVoteResponse response) {
        var result = new Message();
        result.type = MessageType.RAFT_VOTE_RESPONSE.name();
        result.destinationId = response.destinationPeerId();
        result.raftTerm = response.term();
        result.success = response.voteGranted();
        return result;
    }

    public RaftVoteResponse deserializeRaftVoteResponse(Message message) {
        return new RaftVoteResponse(
                message.sourceId,
                message.destinationId,
                message.raftTerm,
                message.success
        );
    }

    public Message serializeSubmitRequest(SubmitRequest request) {
        var result = new Message();
        result.type = MessageType.SUBMIT_REQUEST.name();
        result.requestId = request.requestId();
        result.embeddedMessages = List.of(request.entry());
        result.destinationId = request.destinationId();
        return result;
    }

    public SubmitRequest deserializeSubmitRequest(Message message) {
        Message entry = null;
        if (message.embeddedMessages != null && 0 < message.embeddedMessages.size()) {
            entry = message.embeddedMessages.get(0);
        }
        return new SubmitRequest(
                message.requestId,
                message.destinationId,
                entry
        );
    }

    public Message serializeHelloNotification(HelloNotification notification) {
        var result = new Message();
        result.type = MessageType.HELLO_NOTIFICATION.name();
        result.sourceId = notification.sourcePeerId();
        result.raftLeaderId = notification.raftLeaderId();
        return result;
    }

    public HelloNotification deserializeHelloNotification(Message message) {
        return new HelloNotification(
                message.sourceId,
                message.raftLeaderId
        );
    }

    public Message serializeSubmitResponse(SubmitResponse response) {
        var result = new Message();
        result.type = MessageType.SUBMIT_RESPONSE.name();
        result.requestId = response.requestId();
        result.success = response.success();
        result.raftLeaderId = response.leaderId();
        result.destinationId = response.destinationId();
        return result;
    }

    public SubmitResponse deserializeSubmitResponse(Message response) {
        return new SubmitResponse(
                response.requestId,
                response.destinationId,
                response.success,
                response.raftLeaderId
        );
    }

    public Message serializeStorageSyncRequest(StorageSyncRequest request) {
        var result = new Message();
        result.type = MessageType.STORAGE_SYNC_REQUEST.name();
        result.requestId = request.requestId();
        result.sourceId = request.sourceEndpointId();
        result.destinationId = request.leaderId();
        return result;
    }

    public StorageSyncRequest deserializeStorageSyncRequest(Message request) {
        return new StorageSyncRequest(
                request.requestId,
                request.sourceId,
                request.destinationId
        );
    }

    public StorageSyncResponse deserializeStorageSyncResponse(Message message) {
        var storageUpdateNotifications = new HashMap<String, Message>();
        if (message.keys != null && message.embeddedMessages != null) {
            var length = Math.min(message.keys.size(), message.embeddedMessages.size());
            for (int i = 0; i < length; ++i) {
                var bytes = message.keys.get(i);
                var storageId = bytes != null ? new String(bytes, StandardCharsets.UTF_8) : "";
                storageUpdateNotifications.put(storageId, message.embeddedMessages.get(i));
            }
        }
        return new StorageSyncResponse(
                message.requestId,
                storageUpdateNotifications,
                message.raftCommitIndex,
                message.destinationId,
                message.success,
                message.raftLeaderId,
                message.sequence,
                message.lastMessage
        );
    }

    public Message serializeStorageSyncResponse(StorageSyncResponse response) {
        var result = new Message();
        result.type = MessageType.STORAGE_SYNC_RESPONSE.name();
        result.requestId = response.requestId();
        result.destinationId = response.destinationId();
        result.raftLeaderId = response.leaderId();
        result.raftCommitIndex = response.commitIndex();
        result.sequence = response.sequence();
        result.lastMessage = response.lastMessage();
        result.success = response.success();

        if (response.storageUpdateNotifications() != null) {
            result.keys = new LinkedList<>();
            result.embeddedMessages = new LinkedList<>();
            for (var entry : response.storageUpdateNotifications().entrySet()) {
                var storageId = entry.getKey();
                result.keys.add(storageId.getBytes(StandardCharsets.UTF_8));
                result.embeddedMessages.add(entry.getValue());
            }
        } else {
            result.keys = Collections.emptyList();
            result.embeddedMessages = Collections.emptyList();
        }
        return result;
    }
}
