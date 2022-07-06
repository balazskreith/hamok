package io.github.balazskreith.hamok.transports;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SimpleBufferPool implements BufferPool {
    public Queue<ByteBuffer> packetBufferPool = new ConcurrentLinkedQueue<>();
    public Queue<ByteBuffer> resultBufferPool = new ConcurrentLinkedQueue<>();
    private final int maxPacketLength;
    private final int maxStoredResultLength;

    public SimpleBufferPool(int maxPacketLength, int maxStoredResultLength) {
        this.maxPacketLength = maxPacketLength;
        this.maxStoredResultLength = maxStoredResultLength;
        for (int i = 0; i < 10; ++i) {
            var buffer = ByteBuffer.allocate(this.maxPacketLength);
            this.packetBufferPool.add(buffer);
        }
    }

    public ByteBuffer takePacketBuffer() {
        var result = this.packetBufferPool.poll();
        if (result == null) {
            return ByteBuffer.allocate(this.maxPacketLength);
        }
        result.clear();
        return result;
    }

    public ByteBuffer takeResultBuffer(int capacity) {
        if (this.resultBufferPool.isEmpty()) {
            return ByteBuffer.allocate(capacity);
        }
        for (int i = 0, c = this.resultBufferPool.size(); i < c; ++i) {
            var candidate = this.resultBufferPool.poll();
            if (candidate == null) {
                continue;
            }
            if (capacity <= candidate.capacity()) {
                candidate.clear();
                return candidate;
            }
            this.resultBufferPool.add(candidate);
        }
        return ByteBuffer.allocate(capacity);
    }


    public void givePacketBuffer(ByteBuffer buffer) {
        this.packetBufferPool.add(buffer);
    }

    public void giveResultBuffer(ByteBuffer buffer) {
        if (this.maxStoredResultLength < buffer.capacity()) {
            return;
        }
        this.resultBufferPool.add(buffer);
    }
}
