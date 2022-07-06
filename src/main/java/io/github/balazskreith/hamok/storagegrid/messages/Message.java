package io.github.balazskreith.hamok.storagegrid.messages;

import io.github.balazskreith.hamok.common.Utils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

// oh C union, sweet C union, where are you when we need you the most!!!

/**
 *
 */
public class Message {

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
    public List<UUID> inactiveEndpointIds;

    public List<Message> raftEntries;
    public Boolean success;
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
        result.keys = this.keys != null ? List.copyOf(this.keys) : null;
        result.values = this.values != null ? List.copyOf(this.values) : null;
        result.activeEndpointIds = this.activeEndpointIds != null ? List.copyOf(this.activeEndpointIds) : null;
        result.inactiveEndpointIds = this.inactiveEndpointIds != null ? List.copyOf(this.inactiveEndpointIds) : null;
        result.raftEntries = this.raftEntries != null ? List.copyOf(this.raftEntries) : null;
        result.success = this.success;
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

    @Override
    public String toString() {
        return String.format("{\n" +
                "\tsourceId: %s\n" +
                "\tdestinationId: %s\n" +
                "\tstorageId: %s\n" +
                "\tprotocol: %s\n" +
                "\trequestId: %s\n" +
                "\tstorageSize: %d\n" +
                "\ttimestamp: %d\n" +
                "\ttype: %s\n" +
                "\tkeys: %s\n" +
                "\tvalues: %s\n" +
                "\tactiveEndpointIds: %s\n" +
                "\tinactiveEndpointIds: %s\n" +
                "\traftEntries: %s\n" +
                "\tsuccess: %s\n" +
                "\traftLeaderId: %s\n" +
                "\traftCommitIndex: %d\n" +
                "\traftLeaderNextIndex: %d\n" +
                "\traftPrevLogTerm: %d\n" +
                "\traftPrevLogIndex: %d\n" +
                "\traftTerm: %d\n" +
                "\traftPeerNextIndex: %d\n" +
                "\traftCandidateId: %s\n" +
                "\tsequence: %d\n" +
                "\tlastMessage: %s\n" +
                "}",
                this.sourceId,
                this.destinationId,
                this.storageId,
                this.protocol,
                this.requestId,
                this.storageSize,
                this.timestamp,
                this.type,
                String.join(",", Utils.firstNonNull(keys, Collections.<byte[]>emptyList()).stream().map(b -> new String(b)).collect(Collectors.toList())),
                String.join(",", Utils.firstNonNull(values, Collections.<byte[]>emptyList()).stream().map(String::new).collect(Collectors.toList())),
                String.join(",", Utils.firstNonNull(activeEndpointIds, Collections.emptyList()).stream().map(Object::toString).collect(Collectors.toList())),
                String.join(",", Utils.firstNonNull(inactiveEndpointIds, Collections.emptyList()).stream().map(Object::toString).collect(Collectors.toList())),
                String.join(",", Utils.firstNonNull(raftEntries, Collections.emptyList()).stream().map(Object::toString).collect(Collectors.toList())),
                this.success,
                this.raftLeaderId,
                this.raftCommitIndex,
                this.raftLeaderNextIndex,
                this.raftPrevLogTerm,
                this.raftPrevLogIndex,
                this.raftTerm,
                this.raftPeerNextIndex,
                this.raftCandidateId,
                this.sequence,
                this.lastMessage
            );
    }
}
