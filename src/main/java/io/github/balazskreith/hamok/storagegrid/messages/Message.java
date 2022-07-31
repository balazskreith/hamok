package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.List;
import java.util.UUID;

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

    public List<String> keys;
    public List<String> values;
    public List<UUID> activeEndpointIds;
    public List<UUID> inactiveEndpointIds;

    public List<Message> entries;
    public Boolean success;
    public UUID raftLeaderId;
    public Integer raftCommitIndex;
    public Integer raftLeaderNextIndex;
    public Integer raftPrevLogTerm;
    public Integer raftPrevLogIndex;
    public Integer raftTerm;
    public Integer raftPeerNextIndex;
    public UUID raftCandidateId;

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
        result.entries = this.entries != null ? List.copyOf(this.entries) : null;
        result.success = this.success;
        result.raftLeaderId = this.raftLeaderId;
        result.raftCommitIndex = this.raftCommitIndex;
        result.raftLeaderNextIndex = this.raftLeaderNextIndex;
        result.raftPrevLogTerm = this.raftPrevLogTerm;
        result.raftPrevLogIndex = this.raftPrevLogIndex;
        result.raftTerm = this.raftTerm;
        result.raftPeerNextIndex = this.raftPeerNextIndex;
        result.raftCandidateId = this.raftCandidateId;
        return result;
    }

}
