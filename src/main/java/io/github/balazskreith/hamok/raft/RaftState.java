package io.github.balazskreith.hamok.raft;

public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER,

    NONE,
}
