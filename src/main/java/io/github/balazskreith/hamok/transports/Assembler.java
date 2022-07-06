package io.github.balazskreith.hamok.transports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Assembler {

    private static final Logger logger = LoggerFactory.getLogger(Assembler.class);

    private Map<Integer, Chunk> chunks = new ConcurrentHashMap<>();
    private volatile int endSequence = -1;
    private final BufferPool bufferPool;

    public Assembler(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }


    public void add(DatagramPacket packet) {
        byte end = packet.getData()[0];
        var seqBuf = ByteBuffer.allocate(Integer.BYTES);
        var sequence = seqBuf.put(packet.getData(), 9, Integer.BYTES).flip().getInt();
        if (end == 1) {
            this.endSequence = sequence;
        }
        var copy = this.bufferPool.takePacketBuffer();
//        var copy = ByteBuffer.allocate(packet.getData().length);
        copy.put(packet.getData());
        var prevChunk = this.chunks.put(sequence, new Chunk(
                sequence,
                copy
        ));
        if (prevChunk != null) {
            logger.warn("Assembler for packet for sequence {} twice", sequence);
        }
    }


    public boolean isReady() {
        if (this.endSequence < 0) return false;
        for (int seq = 0; seq <= this.endSequence; ++seq) {
            if (this.chunks.get(seq) == null) return false;
        }
        return true;
    }

    public ByteBuffer assemble() throws Exception {
        int finalSize = 0;
        ArrayList<ByteBuffer> chunks = new ArrayList<>();
        for (int seq = 0; seq <= this.endSequence; ++seq) {
            var chunk = this.chunks.get(seq);
            finalSize += chunk.packetBuffer().position() -  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH;
            chunks.add(chunk.packetBuffer());
        }
        var buffer = this.bufferPool.takeResultBuffer(finalSize);
//        var buffer = ByteBuffer.allocate(finalSize);
        for (var chunk : chunks) {
            buffer.put(chunk.array(),  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH, chunk.position() -  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH);
            this.bufferPool.givePacketBuffer(chunk);
        }

        return buffer;
    }
}
