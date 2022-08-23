package io.github.balazskreith.hamok.storagegrid.messages;

import io.github.balazskreith.hamok.common.Utils;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

// oh C union, sweet C union, where are you when we need you the most!!!

/**
 *
 */
public class Message {

    public Message() {
        this.timestamp = Instant.now().toEpochMilli();
    }

    /**
     * The source endpoint
     */
    public UUID sourceId;
    /**
     * The destination endpoint. if null, then broadcast
     */
    public UUID destinationId;

    public String storageId;
    /**
     * Under one storage there can be several protocols (i.e.: backups). this is for that
     */
    public String protocol;
    public UUID requestId;
    public Integer storageSize;
    public Long timestamp;

    public String type;

    public List<byte[]> keys;
    public List<byte[]> values;
    public List<UUID> activeEndpointIds;

    public List<Message> embeddedMessages;
    public Boolean success;
    public Boolean executeSync;
    public UUID raftLeaderId;
    public Integer raftCommitIndex;
    public Integer raftLeaderNextIndex;
    public Integer raftPrevLogTerm;
    public Integer raftPrevLogIndex;
    public Integer raftTerm;
    public Integer raftPeerNextIndex;
    public UUID raftCandidateId;

    public Integer sequence;
    public Boolean lastMessage;

    public Message makeCopy() {
        var result = new Message();
        result.sourceId = this.sourceId;
        result.destinationId = this.destinationId;
        result.storageId = this.storageId;
        result.protocol = this.protocol;
        result.requestId = this.requestId;
        result.storageSize = this.storageSize;
        result.timestamp = this.timestamp;
        result.type = this.type;
        result.keys = this.keys != null ? this.keys : null;
        result.values = this.values != null ? this.values : null;
        result.activeEndpointIds = this.activeEndpointIds != null ? this.activeEndpointIds : null;
        result.embeddedMessages = this.embeddedMessages != null ? this.embeddedMessages : null;
        result.success = this.success;
        result.executeSync = this.executeSync;
        result.raftLeaderId = this.raftLeaderId;
        result.raftCommitIndex = this.raftCommitIndex;
        result.raftLeaderNextIndex = this.raftLeaderNextIndex;
        result.raftPrevLogTerm = this.raftPrevLogTerm;
        result.raftPrevLogIndex = this.raftPrevLogIndex;
        result.raftTerm = this.raftTerm;
        result.raftPeerNextIndex = this.raftPeerNextIndex;
        result.raftCandidateId = this.raftCandidateId;
        result.sequence = this.sequence;
        result.lastMessage = this.lastMessage;
        return result;
    }

    public Message assign(Message source) {
        this.sourceId = source.sourceId != null ? source.sourceId : this.sourceId;
        this.destinationId = source.destinationId != null ? source.destinationId : this.destinationId;
        this.storageId = source.storageId != null ? source.storageId : this.storageId;
        this.protocol = source.protocol != null ? source.protocol : this.protocol;
        this.requestId = source.requestId != null ? source.requestId : this.requestId;
        this.storageSize = source.storageSize != null ? source.storageSize : this.storageSize;
        this.timestamp = source.timestamp != null ? source.timestamp : this.timestamp;
        this.type = source.type != null ? source.type : this.type;
        this.keys = source.keys != null ? source.keys : this.keys;
        this.values = source.values != null ? source.values : this.values;
        this.activeEndpointIds = source.activeEndpointIds != null ? source.activeEndpointIds : this.activeEndpointIds;
        this.embeddedMessages = source.embeddedMessages != null ? source.embeddedMessages : this.embeddedMessages;
        this.success = source.success != null ? source.success : this.success;
        this.executeSync = source.executeSync != null ? source.executeSync : this.executeSync;
        this.raftLeaderId = source.raftLeaderId != null ? source.raftLeaderId : this.raftLeaderId;
        this.raftCommitIndex = source.raftCommitIndex != null ? source.raftCommitIndex : this.raftCommitIndex;
        this.raftLeaderNextIndex = source.raftLeaderNextIndex != null ? source.raftLeaderNextIndex : this.raftLeaderNextIndex;
        this.raftPrevLogTerm = source.raftPrevLogTerm != null ? source.raftPrevLogTerm : this.raftPrevLogTerm;
        this.raftPrevLogIndex = source.raftPrevLogIndex != null ? source.raftPrevLogIndex : this.raftPrevLogIndex;
        this.raftTerm = source.raftTerm != null ? source.raftTerm : this.raftTerm;
        this.raftPeerNextIndex = source.raftPeerNextIndex != null ? source.raftPeerNextIndex : this.raftPeerNextIndex;
        this.raftCandidateId = source.raftCandidateId != null ? source.raftCandidateId : this.raftCandidateId;
        this.sequence = source.sequence != null ? source.sequence : this.sequence;
        this.lastMessage = source.lastMessage != null ? source.lastMessage : this.lastMessage;
        return this;
    }


    @Override
    public String toString() {
        return this.toString(0);
    }

    public String toString(int indent) {
        Function<Boolean, String> booleanToString = b -> Boolean.TRUE.equals(b) ? "true" : "false";
        var mainTab = "\t".repeat(indent);
        var innerTab = mainTab + "\t";
        return String.format("{%s \n" +
                "%s sourceId: %s\n" +
                "%s destinationId: %s\n" +
                "%s storageId: %s\n" +
                "%s protocol: %s\n" +
                "%s requestId: %s\n" +
                "%s storageSize: %d\n" +
                "%s timestamp: %s\n" +
                "%s type: %s\n" +
                "%s keys: (number of keys: %d)\n" +
                "%s values: (number of values: %d)\n" +
                "%s activeEndpointIds: %s\n" +
                "%s embeddedMessages: %s\n" +
                "%s success: %s\n" +
                "%s executeSync: %s\n" +
                "%s raftLeaderId: %s\n" +
                "%s raftCommitIndex: %d\n" +
                "%s raftLeaderNextIndex: %d\n" +
                "%s raftPrevLogTerm: %d\n" +
                "%s raftPrevLogIndex: %d\n" +
                "%s raftTerm: %d\n" +
                "%s raftPeerNextIndex: %d\n" +
                "%s raftCandidateId: %s\n" +
                "%s sequence: %d\n" +
                "%s lastMessage: %s\n" +
                "}",
                mainTab,
                innerTab, this.sourceId,
                innerTab, this.destinationId,
                innerTab, this.storageId,
                innerTab, this.protocol,
                innerTab, this.requestId,
                innerTab, this.storageSize,
                innerTab, Instant.ofEpochMilli(this.timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime(),
                innerTab, this.type,
                innerTab, Utils.firstNonNull(keys, Collections.<byte[]>emptyList()).size(),
                innerTab, Utils.firstNonNull(values, Collections.<byte[]>emptyList()).size(),
                innerTab, String.join(",", Utils.firstNonNull(activeEndpointIds, Collections.emptyList()).stream().map(Object::toString).collect(Collectors.toList())),
                innerTab, String.join(",", Utils.firstNonNull(embeddedMessages, Collections.<Message>emptyList()).stream().map(msg -> msg.toString(indent + 1)).collect(Collectors.toList())),
                innerTab, booleanToString.apply(this.success),
                innerTab, booleanToString.apply(this.executeSync),
                innerTab, this.raftLeaderId,
                innerTab, this.raftCommitIndex,
                innerTab, this.raftLeaderNextIndex,
                innerTab, this.raftPrevLogTerm,
                innerTab, this.raftPrevLogIndex,
                innerTab, this.raftTerm,
                innerTab, this.raftPeerNextIndex,
                innerTab, this.raftCandidateId,
                innerTab, this.sequence,
                innerTab, this.lastMessage
            );
    }


}
