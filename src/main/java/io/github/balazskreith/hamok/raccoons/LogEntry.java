package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.storagegrid.messages.Message;

public record LogEntry(int index, int term, Message entry, Long timestamp) {

    @Override
    public String toString() {
        return String.format("index: %d, term: %d, bytes: %s", index, term, this.entry);
    }
}
