package io.github.balazskreith.vstorage.raft;

public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER,

    NONE,
}
