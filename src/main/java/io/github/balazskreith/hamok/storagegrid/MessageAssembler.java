package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;

public interface MessageAssembler {
    boolean isReady();
    void process(Models.Message message);
    Models.Message getResult();
}
