package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;

public interface MessageAssembler {
    boolean isReady();
    void process(Message message);
    Message getResult();
}
