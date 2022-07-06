package io.github.balazskreith.hamok.transports;

import java.nio.ByteBuffer;

public record Chunk(int index, ByteBuffer packetBuffer) {
}
