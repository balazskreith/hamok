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
    private Map<String, Assembler> assemblers;

    public Depacketizer(Decoder<byte[], Message> decoder) {
        this.assemblers = new ConcurrentHashMap<>();
        this.decoder = decoder;
    }

    public Message decode(DatagramPacket packet) {
        String source = String.format("%s:%d", packet.getAddress().toString(), packet.getPort());
        var assembler = this.assemblers.get(source);
        byte end = packet.getData()[0];
        if (end == 1 && assembler == null) {
            var messageBytes = ByteBuffer.allocate(packet.getLength() - DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH);
            messageBytes.put(packet.getData(),  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH, packet.getLength() -  DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH);
            try {
                return this.decoder.decode(messageBytes.array());
            } catch (Throwable e) {
                logger.warn("Error occurred while decoding packet bytes", e);
                return null;
            }
        }
        if (assembler == null) {
            assembler = new Assembler();
            this.assemblers.put(source, assembler);
        }
        assembler.add(packet);
        if (!assembler.isReady()) {
            return null;
        }

        byte[] messageBytes;
        try {
            messageBytes = assembler.assemble();
        } catch (Exception e) {
            logger.warn("Error occurred while assembling chunks");
            this.assemblers.remove(source);
            return null;
        }
        try {
            return this.decoder.decode(messageBytes);
        } catch (Throwable e) {
            logger.warn("Error occurred while decoding packet bytes", e);
            return null;
        }
    }
}
