package io.github.balazskreith.hamok.raft;

import java.util.Base64;

public record LogEntry(int index, int term, byte[] entry) {

    @Override
    public String toString() {
        return String.format("index: %d, term: %d, bytes: %s", index, term, Base64.getEncoder().encodeToString(entry));
    }
}
