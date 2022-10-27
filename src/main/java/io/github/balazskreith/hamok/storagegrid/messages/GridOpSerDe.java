package io.github.balazskreith.hamok.storagegrid.messages;


import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.Utils;
import io.github.balazskreith.hamok.raccoons.events.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

public class GridOpSerDe {

    private static final Logger logger = LoggerFactory.getLogger(GridOpSerDe.class);

    public Models.Message.Builder serializeEndpointStatesNotification(EndpointStatesNotification notification) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.ENDPOINT_STATES_NOTIFICATION.name())
                ;
        if (notification.sourceEndpointId() != null) {
            result.setSourceId(notification.sourceEndpointId().toString());
        }
        if (notification.destinationEndpointId() != null) {
            result.setDestinationId(notification.destinationEndpointId().toString());
        }
        if (notification.activeEndpointIds() != null) {
            result.addAllActiveEndpointIds(notification.activeEndpointIds().stream().map(Object::toString).collect(Collectors.toList()));
        }
        Utils.relayIfNotNull(notification::commitIndex, result::setRaftCommitIndex);
        Utils.relayIfNotNull(notification::leaderNextIndex, result::setRaftLeaderNextIndex);
        Utils.relayIfNotNull(notification::numberOfLogs, result::setRaftNumberOfLogs);
        Utils.relayIfNotNull(notification::numberOfLogs, result::setRaftNumberOfLogs);
        Utils.relayIfNotNull(notification::term, result::setRaftTerm);
        return result;
    }

    public EndpointStatesNotification deserializeEndpointStatesNotification(Models.Message message) {
        var activeEndpointIds = Utils.supplyIfTrueOrElse(0 < message.getActiveEndpointIdsCount(),
                () -> message.getActiveEndpointIdsList().stream().map(UUID::fromString).collect(Collectors.toSet()),
                Collections.<UUID>emptySet()
        );
        return new EndpointStatesNotification(
                Utils.supplyIfTrue(message.hasSourceId(), () -> UUID.fromString(message.getSourceId())),
                activeEndpointIds,
                Utils.supplyIfTrue(message.hasRaftNumberOfLogs(), message::getRaftNumberOfLogs),
                Utils.supplyIfTrue(message.hasRaftLeaderNextIndex(), message::getRaftLeaderNextIndex),
                Utils.supplyIfTrue(message.hasRaftCommitIndex(), message::getRaftCommitIndex),
                Utils.supplyIfTrue(message.hasDestinationId(), () -> UUID.fromString(message.getDestinationId())),
                Utils.supplyIfTrue(message.hasRaftTerm(), message::getRaftTerm)
        );
    }

    public Models.Message.Builder serializeRaftAppendRequestChunk(RaftAppendEntriesRequestChunk request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.RAFT_APPEND_ENTRIES_REQUEST_CHUNK.name())
                ;
        Utils.relayMappedIfNotNull(request::peerId, UUID::toString, result::setDestinationId);
        Utils.relayMappedIfNotNull(request::leaderId, UUID::toString, result::setRaftLeaderId);
        Utils.relayIfNotNull(request::leaderCommit, result::setRaftCommitIndex);
        Utils.relayIfNotNull(request::leaderNextIndex, result::setRaftLeaderNextIndex);
        Utils.relayIfNotNull(request::prevLogIndex, result::setRaftPrevLogIndex);
        Utils.relayIfNotNull(request::prevLogTerm, result::setRaftPrevLogTerm);
        Utils.relayIfNotNull(request::term, result::setRaftTerm);
        Utils.relayIfNotNull(request::entry, result::addEmbeddedMessages);
        Utils.relayIfNotNull(request::sequence, result::setSequence);
        Utils.relayMappedIfNotNull(request::requestId, UUID::toString, result::setRequestId);
        Utils.relayIfNotNull(request::lastMessage, result::setLastMessage);
        return result;
    }

    public RaftAppendEntriesRequestChunk deserializeRaftAppendRequestChunk(Models.Message message) {
        Models.Message entry = null;
        if (0 < message.getEmbeddedMessagesCount()) {
            entry = message.getEmbeddedMessages(0);
            if (1 < message.getEmbeddedMessagesCount()) {
                logger.warn("More than one message received for RaftAppendRequestChunk. Only the first one will be processed");
            }
        }
        return new RaftAppendEntriesRequestChunk(
                Utils.supplyMappedIfTrue(message.hasDestinationId(), message::getDestinationId, UUID::fromString),
                Utils.supplyIfTrueOrElse(message.hasRaftTerm(), message::getRaftTerm, 0),
                Utils.supplyMappedIfTrue(message.hasRaftLeaderId(), message::getRaftLeaderId, UUID::fromString),
                Utils.supplyIfTrueOrElse(message.hasRaftPrevLogIndex(), message::getRaftPrevLogIndex, 0),
                Utils.supplyIfTrueOrElse(message.hasRaftPrevLogTerm(), message::getRaftPrevLogTerm, 0),
                entry,
                Utils.supplyIfTrueOrElse(message.hasRaftCommitIndex(), message::getRaftCommitIndex, 0),
                Utils.supplyIfTrueOrElse(message.hasRaftLeaderNextIndex(), message::getRaftLeaderNextIndex, 0),
                Utils.supplyIfTrueOrElse(message.hasSequence(), message::getSequence, 0),
                Utils.supplyIfTrueOrElse(message.hasLastMessage(), message::getLastMessage, false),
                Utils.supplyMappedIfTrue(message.hasRequestId(), message::getRequestId, UUID::fromString)
        );
    }

    public Models.Message.Builder serializeRaftAppendResponse(RaftAppendEntriesResponse response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.RAFT_APPEND_ENTRIES_RESPONSE.name())
                .setSuccess(response.success())
                .setRaftTerm(response.term())
                .setRaftPeerNextIndex(response.peerNextIndex())
                .setLastMessage(response.processed());
        Utils.relayMappedIfNotNull(response::requestId, UUID::toString, result::setRequestId);
        Utils.relayMappedIfNotNull(response::destinationPeerId, UUID::toString, result::setDestinationId);
        Utils.relayMappedIfNotNull(response::sourcePeerId, UUID::toString, result::setSourceId);
        return result;
    }

    public RaftAppendEntriesResponse deserializeRaftAppendResponse(Models.Message message) {
        return new RaftAppendEntriesResponse(
                Utils.supplyMappedIfTrue(message.hasSourceId(), message::getSourceId, UUID::fromString),
                Utils.supplyMappedIfTrue(message.hasDestinationId(), message::getDestinationId, UUID::fromString),
                Utils.supplyMappedIfTrue(message.hasRequestId(), message::getRequestId, UUID::fromString),
                Utils.supplyIfTrueOrElse(message.hasRaftTerm(), message::getRaftTerm, -1),
                Utils.supplyIfTrueOrElse(message.hasSuccess(), message::getSuccess, false),
                Utils.supplyIfTrueOrElse(message.hasRaftPeerNextIndex(), message::getRaftPeerNextIndex, -1),
                Utils.supplyIfTrueOrElse(message.hasLastMessage(), message::getLastMessage, false)
        );
    }

    public Models.Message.Builder serializeRaftVoteRequest(RaftVoteRequest request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.RAFT_VOTE_REQUEST.name())
                .setRaftTerm(request.term())
                .setRaftPrevLogIndex(request.lastLogIndex())
                .setRaftPrevLogTerm(request.lastLogTerm())
                ;
        Utils.relayUuidToStringIfNotNull(request::peerId, result::setDestinationId);
        Utils.relayUuidToStringIfNotNull(request::candidateId, result::setRaftCandidateId);
        return result;
    }

    public RaftVoteRequest deserializeRaftVoteRequest(Models.Message message) {
        return new RaftVoteRequest(
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId),
                Utils.supplyIfTrueOrElse(message.hasRaftTerm(), message::getRaftTerm, -1),
                Utils.supplyStringToUuidIfTrue(message.hasRaftCandidateId(), message::getRaftCandidateId),
                Utils.supplyIfTrueOrElse(message.hasRaftPrevLogIndex(), message::getRaftPrevLogIndex, -1),
                Utils.supplyIfTrueOrElse(message.hasRaftPrevLogTerm(), message::getRaftPrevLogTerm, -1)
        );
    }

    public Models.Message.Builder serializeRaftVoteResponse(RaftVoteResponse response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.RAFT_VOTE_RESPONSE.name())
                .setRaftTerm(response.term())
                .setSuccess(response.voteGranted())
                ;
        Utils.relayUuidToStringIfNotNull(response::destinationPeerId, result::setDestinationId);
        Utils.relayUuidToStringIfNotNull(response::sourcePeerId, result::setSourceId);
        return result;
    }

    public RaftVoteResponse deserializeRaftVoteResponse(Models.Message message) {
        return new RaftVoteResponse(
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId),
                Utils.supplyIfTrueOrElse(message.hasRaftTerm(), message::getRaftTerm, -1),
                Utils.supplyIfTrueOrElse(message.hasSuccess(), message::getSuccess, false)
        );
    }

    public Models.Message.Builder serializeSubmitRequest(SubmitRequest request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.SUBMIT_REQUEST.name())
        ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(request::destinationId, result::setDestinationId);
        Utils.relayIfNotNull(request::entry, result::addEmbeddedMessages);
        return result;
    }

    public SubmitRequest deserializeSubmitRequest(Models.Message message) {
        Models.Message entry = null;
        if (0 < message.getEmbeddedMessagesCount()) {
            entry = message.getEmbeddedMessages(0);
        }
        return new SubmitRequest(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId),
                entry
        );
    }

    public Models.Message.Builder serializeHelloNotification(HelloNotification notification) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.HELLO_NOTIFICATION.name())
                ;
        Utils.relayUuidToStringIfNotNull(notification::sourcePeerId, result::setSourceId);
        Utils.relayUuidToStringIfNotNull(notification::raftLeaderId, result::setRaftLeaderId);
        return result;
    }

    public HelloNotification deserializeHelloNotification(Models.Message message) {
        return new HelloNotification(
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId),
                Utils.supplyStringToUuidIfTrue(message.hasRaftLeaderId(), message::getRaftLeaderId)
        );
    }

    public Models.Message.Builder serializeSubmitResponse(SubmitResponse response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.SUBMIT_RESPONSE.name())
                .setSuccess(response.success())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(response::leaderId, result::setRaftLeaderId);
        Utils.relayUuidToStringIfNotNull(response::destinationId, result::setDestinationId);
        return result;
    }

    public SubmitResponse deserializeSubmitResponse(Models.Message response) {
        return new SubmitResponse(
                Utils.supplyStringToUuidIfTrue(response.hasRequestId(), response::getRequestId),
                Utils.supplyStringToUuidIfTrue(response.hasDestinationId(), response::getDestinationId),
                Utils.supplyIfTrueOrElse(response.hasSuccess(), response::getSuccess, false),
                Utils.supplyStringToUuidIfTrue(response.hasRaftLeaderId(), response::getRaftLeaderId)
        );
    }

    public Models.Message.Builder serializeStorageSyncRequest(StorageSyncRequest request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.STORAGE_SYNC_REQUEST.name())
                ;

        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        Utils.relayUuidToStringIfNotNull(request::leaderId, result::setDestinationId);
        return result;
    }

    public StorageSyncRequest deserializeStorageSyncRequest(Models.Message request) {
        return new StorageSyncRequest(
                Utils.supplyStringToUuidIfTrue(request.hasRequestId(), request::getRequestId),
                Utils.supplyStringToUuidIfTrue(request.hasSourceId(), request::getSourceId),
                Utils.supplyStringToUuidIfTrue(request.hasDestinationId(), request::getDestinationId)
        );
    }

    public StorageSyncResponse deserializeStorageSyncResponse(Models.Message message) {
        return new StorageSyncResponse(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId),
                Utils.supplyStringToUuidIfTrue(message.hasRaftLeaderId(), message::getRaftLeaderId),
                Utils.supplyIfTrue(message.hasRaftNumberOfLogs(), message::getRaftNumberOfLogs),
                Utils.supplyIfTrue(message.hasRaftLastAppliedIndex(), message::getRaftLastAppliedIndex),
                Utils.supplyIfTrue(message.hasRaftCommitIndex(), message::getRaftCommitIndex)
        );
    }

    public Models.Message.Builder serializeStorageSyncResponse(StorageSyncResponse response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.STORAGE_SYNC_RESPONSE.name())
                ;

        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(response::destinationId, result::setDestinationId);
        Utils.relayUuidToStringIfNotNull(response::leaderId, result::setRaftLeaderId);
        Utils.relayIfNotNull(response::numberOfLogs, result::setRaftNumberOfLogs);
        Utils.relayIfNotNull(response::lastApplied, result::setRaftLastAppliedIndex);
        Utils.relayIfNotNull(response::commitIndex, result::setRaftCommitIndex);
        return result;
    }
}
