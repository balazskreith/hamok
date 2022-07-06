package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.mappings.Encoder;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

public class Packetizer {

    private static final Logger logger = LoggerFactory.getLogger(Packetizer.class);

    private final Encoder<Message, byte[]> encoder;

    public Packetizer(Encoder<Message, byte[]> encoder) {
        this.encoder = encoder;
    }

    public Iterator<byte[]> encode(Message message) {
        byte[] messageBytes;
        try {
            messageBytes = this.encoder.encode(message);
        } catch (Throwable e) {
            logger.warn("Error occurred while encoding message to bytes", e);
            return EMPTY_ITERATOR;
        }
        long ssrc = UUID.randomUUID().getLeastSignificantBits();
        return new Iterator<byte[]>() {
            int seq = 0;
            int offset = 0;
            @Override
            public boolean hasNext() {
                return this.offset < messageBytes.length;
            }

            @Override
            public byte[] next() {
                int bufferSize = Math.min(messageBytes.length - this.offset, DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH);
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize + DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH);
                buffer.put(
                        offset + bufferSize == messageBytes.length ? (byte)1 : (byte)0
                );
                buffer.putLong(ssrc);
                buffer.putInt(seq);
                buffer.put(
                        messageBytes, offset, bufferSize
                );
                this.offset +=  DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH;
                ++this.seq;
                return buffer.array();
            }
        };
    }


    private static final Iterator<byte[]> EMPTY_ITERATOR = new Iterator<byte[]>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public byte[] next() {
            return null;
        }
    };
}
