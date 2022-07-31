package io.github.balazskreith.hamok.storagegrid.messages;


import io.github.balazskreith.hamok.raccoons.events.*;

import java.util.*;
import java.util.stream.Collectors;

public class GridOpSerDe {

    private final Base64.Encoder base64Encoder = Base64.getEncoder();
    private final Base64.Decoder base64Decoder = Base64.getDecoder();

    public Message serializeEndpointStatesNotification(EndpointStatesNotification notification) {
        var result = new Message();
        result.type = MessageType.ENDPOINT_STATES_NOTIFICATION.name();
        result.sourceId = notification.sourceEndpointId();
        result.destinationId = notification.destinationEndpointId();
        result.activeEndpointIds = notification.activeEndpointIds().stream().collect(Collectors.toList());
        result.inactiveEndpointIds = notification.inactiveEndpointIds().stream().collect(Collectors.toList());
        return result;
    }

    public EndpointStatesNotification deserializeEndpointStatesNotification(Message message) {
        var activeEndpointIds = message.activeEndpointIds.stream().collect(Collectors.toSet());
        var inactiveEndpointIds = message.inactiveEndpointIds.stream().collect(Collectors.toSet());
        return new EndpointStatesNotification(
                message.sourceId,
                activeEndpointIds,
                inactiveEndpointIds,
                message.destinationId
        );
    }

    public Message serializeRaftAppendRequest(RaftAppendEntriesRequest request) {
        var result = new Message();
        result.type = MessageType.RAFT_APPEND_ENTRIES_REQUEST.name();
        result.destinationId = request.peerId();
        result.raftLeaderId = request.leaderId();
        result.raftCommitIndex = request.leaderCommit();
        result.raftLeaderNextIndex = request.leaderNextIndex();
        result.raftPrevLogTerm = request.prevLogTerm();
        result.raftPrevLogIndex = request.prevLogIndex();
        result.raftTerm = request.term();
        result.entries = request.entries();
        return result;
    }

    public RaftAppendEntriesRequest deserializeRaftAppendRequest(Message message) {
        return new RaftAppendEntriesRequest(
                message.destinationId,
                message.raftTerm,
                message.raftLeaderId,
                message.raftPrevLogIndex,
                message.raftPrevLogTerm,
                message.entries,
                message.raftCommitIndex,
                message.raftLeaderNextIndex
        );
    }

    public Message serializeRaftAppendResponse(RaftAppendEntriesResponse response) {
        var result = new Message();
        result.type = MessageType.RAFT_APPEND_ENTRIES_RESPONSE.name();
        result.success = response.success();
        result.raftTerm = response.term();
        result.destinationId = response.destinationPeerId();
        result.raftPeerNextIndex = response.peerNextIndex();
        return result;
    }

    public RaftAppendEntriesResponse deserializeRaftAppendResponse(Message message) {
        return new RaftAppendEntriesResponse(
                message.sourceId,
                message.destinationId,
                message.raftTerm,
                message.success,
                message.raftPeerNextIndex
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
        result.entries = List.of(request.entry());
        result.destinationId = request.destinationId();
        return result;
    }

    public SubmitRequest deserializeSubmitRequest(Message message) {
        Message entry = null;
        if (message.entries != null && 0 < message.entries.size()) {
            entry = message.entries.get(0);
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
        var storageUpdateNotifications = new HashMap<String, byte[]>();
        if (message.keys != null && message.values != null) {
            var length = Math.min(message.keys.size(), message.values.size());
            for (int i = 0; i < length; ++i) {
                var storageId = message.keys.get(i);
                var encodedBytes = message.values.get(i);
                var messages = this.base64Decoder.decode(encodedBytes);
                storageUpdateNotifications.put(storageId, messages);
            }
        }
        return new StorageSyncResponse(
                message.requestId,
                storageUpdateNotifications,
                message.raftCommitIndex,
                message.destinationId,
                message.success,
                message.raftLeaderId
        );
    }

    public Message serializeStorageSyncResponse(StorageSyncResponse response) {
        var result = new Message();
        result.type = MessageType.STORAGE_SYNC_RESPONSE.name();
        result.requestId = response.requestId();
        result.destinationId = response.destinationId();
        result.raftLeaderId = response.leaderId();
        result.raftCommitIndex = response.commitIndex();
        result.success = response.success();
        if (response.storageUpdateNotifications() != null) {
            result.keys = new LinkedList<>();
            result.values = new LinkedList<>();
            for (var entry : response.storageUpdateNotifications().entrySet()) {
                var storageId = entry.getKey();
                var messages = entry.getValue();
                var encoded = this.base64Encoder.encodeToString(messages);
                result.keys.add(storageId);
                result.values.add(encoded);
            }
        } else {
            result.keys = Collections.emptyList();
            result.values = Collections.emptyList();
        }
        return result;
    }
}
