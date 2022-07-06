package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.mappings.Decoder;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Depacketizer {

    private static final Logger logger = LoggerFactory.getLogger(Depacketizer.class);

    private Decoder<byte[], Message> decoder;
    private Map<Long, Assembler> assemblers;
    private final BufferPool bufferPool;

    public Depacketizer(Decoder<byte[], Message> decoder) {
        this(decoder, BufferPool.createDummy(
                DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH + DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH
        ));
    }

    public Depacketizer(Decoder<byte[], Message> decoder, BufferPool bufferPool) {
        this.assemblers = new ConcurrentHashMap<>();
        this.decoder = decoder;
        this.bufferPool = bufferPool;
//        this.bufferPool = new SimpleBufferPool(
//                DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH + DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH,
//                DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH * 1000
//        );
    }

    public Message decode(DatagramPacket packet) {
        var ssrcBuf = ByteBuffer.allocate(Long.BYTES);
        var ssrc = ssrcBuf.put(packet.getData(), 1, Long.BYTES).flip().getLong();
        var assembler = this.assemblers.get(ssrc);
        byte end = packet.getData()[0];
        if (end == 1 && assembler == null) {
            var messageBytes = this.bufferPool.takePacketBuffer();
//            var messageBytes = ByteBuffer.allocate(packet.getLength() - DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH);
            try {
                messageBytes.put(packet.getData(),  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH, packet.getData().length -  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH);
                return this.decoder.decode(messageBytes.array());
            } catch (Throwable e) {
                logger.warn("Error occurred while decoding packet bytes", e);
                return null;
            } finally {
                this.bufferPool.givePacketBuffer(messageBytes);
            }
        }
        if (assembler == null) {
            assembler = new Assembler(bufferPool);
            this.assemblers.put(ssrc, assembler);
        }
        assembler.add(packet);
        if (!assembler.isReady()) {
            return null;
        }

        ByteBuffer messageBytes;
        try {
            messageBytes = assembler.assemble();
        } catch (Exception e) {
            logger.warn("Error occurred while assembling chunks", e);
            this.assemblers.remove(ssrc);
            return null;
        }
        try {
            return this.decoder.decode(messageBytes.array());
        } catch (Throwable e) {
            logger.warn("Error occurred while decoding packet bytes", e);
            return null;
        } finally {
            this.bufferPool.giveResultBuffer(messageBytes);
        }
    }
}
