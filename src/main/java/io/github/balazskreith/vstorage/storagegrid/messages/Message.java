package io.github.balazskreith.vstorage.storagegrid.messages;

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
    public Long timestamp;

    public String type;

    public List<String> keys;
    public List<String> values;
    public List<UUID> activeEndpointIds;
    public List<UUID> inactiveEndpointIds;

    public List<String> entries;
    public Boolean success;
    public UUID raftLeaderId;
    public Integer raftCommitIndex;
    public Integer raftLeaderNextIndex;
    public Integer raftPrevLogTerm;
    public Integer raftPrevLogIndex;
    public Integer raftTerm;
    public Integer raftPeerNextIndex;
    public UUID raftCandidateId;
}
