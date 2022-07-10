package io.github.balazskreith.hamok.transports;

public record Chunk(int index, byte[] packetBytes) {
}
