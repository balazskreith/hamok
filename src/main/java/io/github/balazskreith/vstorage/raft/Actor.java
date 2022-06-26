package io.github.balazskreith.vstorage.raft;

import io.reactivex.rxjava3.disposables.Disposable;

import java.util.UUID;

public interface Actor extends Disposable {

    RaftState getState();

    void start();

    void stop();

    void removedPeerId(UUID peerId);

    Integer submit(byte[] entry);

}
