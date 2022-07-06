package io.github.balazskreith.hamok.transports;

import java.nio.ByteBuffer;

public interface BufferPool {

    ByteBuffer takePacketBuffer();
    ByteBuffer takeResultBuffer(int capacity);
    void givePacketBuffer(ByteBuffer buffer);
    void giveResultBuffer(ByteBuffer buffer);

    static BufferPool createDummy(int packetLength) {
        return new BufferPool() {
            @Override
            public ByteBuffer takePacketBuffer() {
                return ByteBuffer.allocate(packetLength);
            }

            @Override
            public ByteBuffer takeResultBuffer(int capacity) {
                return ByteBuffer.allocate(capacity);
            }

            @Override
            public void givePacketBuffer(ByteBuffer buffer) {

            }

            @Override
            public void giveResultBuffer(ByteBuffer buffer) {

            }
        };
    }
}
