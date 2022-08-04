package io.github.balazskreith.hamok.emulators.socketmulticast;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PurgatoryMulticastEndpoint implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(PurgatoryMulticastEndpoint.class);
    private static final int MAX_BUFFER_SIZE = 1200;
    private static final int PACKET_INFO_LENGTH = 17; // 1 byte for end, 16 byte for uuid
    private static final int MAX_PACKET_SIZE = MAX_BUFFER_SIZE + PACKET_INFO_LENGTH;

    private final Subject<Message> inboundMessage = PublishSubject.create();
    private final Subject<Message> outboundMessage = PublishSubject.create();
    private final CompositeDisposable disposer = new CompositeDisposable();
    private final UUID localEndpointId;
    private MulticastSocket inboundSocket;
    private DatagramSocket outboundSocket;
    private final InetAddress group;
    private final int port;
    private final Thread thread;
    private volatile boolean run = true;
    private Map<UUID, List<byte[]>> chunks = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();


    PurgatoryMulticastEndpoint(InetAddress group, int port, UUID localEndpointId) throws IOException {
        this.group = group; // InetAddress.getByName("230.0.0.0");
        this.port = port;
        this.localEndpointId = localEndpointId;
        this.inboundSocket = new MulticastSocket(port);
        this.outboundSocket = new DatagramSocket();
        this.inboundSocket.joinGroup(this.group);
        this.disposer.add(this.outboundMessage.subscribe(message -> {
            logger.info("Outbound message from {} to {} type: {}, protocol {}", message.sourceId, message.destinationId, message.type, message.protocol);
            byte[] messageBytes = this.mapper.writeValueAsBytes(message);
            for (int offset = 0; offset < messageBytes.length; offset += MAX_BUFFER_SIZE) {
                int bufferSize = Math.min(messageBytes.length - offset, MAX_BUFFER_SIZE);
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize + PACKET_INFO_LENGTH);
                buffer.put(
                        offset + bufferSize == messageBytes.length ? (byte)1 : (byte)0
                );
                buffer.put(
                        new UUIDToBytes(message.sourceId).bytes
                );
                buffer.put(
                        messageBytes, offset, bufferSize
                );
                var packet = new DatagramPacket(buffer.array(), 0, bufferSize + PACKET_INFO_LENGTH, group, port);
                logger.warn("Packet to {}:{}", packet.getAddress(), packet.getPort());
                this.outboundSocket.send(packet);
            }
        }));
        this.thread = new Thread(this::receive);
        this.disposer.add(Disposable.fromRunnable(() -> {
            this.run = false;
            try {
                this.thread.join(2000);
            } catch (InterruptedException e) {
                return;
            }
            if (!this.inboundSocket.isClosed()) {
                if (!this.thread.isInterrupted()) {
                    this.thread.interrupt();
                    try {
                        this.inboundSocket.leaveGroup(this.group);
                        this.inboundSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }));
        this.thread.start();
    }

    public Observer<Message> sender() {
        return this.outboundMessage;
    }

    public Observable<Message> receiver() {
        return this.inboundMessage;
    }

    private void receive() {
        byte[] buf = new byte[MAX_PACKET_SIZE];
        while (this.run) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                this.inboundSocket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
            logger.warn("Packet from {}:{}", packet.getAddress(), packet.getPort());
            byte end = packet.getData()[0];
            UUID sourceId = new BytesToUUID(packet.getData(), 1).uuid;
            var chunks = this.chunks.get(sourceId);
            if (chunks == null && end == 1) {
                var messageBytes = ByteBuffer.allocate(packet.getLength() - PACKET_INFO_LENGTH);
                messageBytes.put(packet.getData(), PACKET_INFO_LENGTH, packet.getLength() - PACKET_INFO_LENGTH);
                this.forward(messageBytes.array());
                continue;
            }
            if (chunks == null) {
                chunks = new LinkedList<>();
                this.chunks.put(sourceId, chunks);
            }
            if (end == 0) {
                byte[] copied = Arrays.copyOf(packet.getData(), packet.getLength());
                chunks.add(copied);
                continue;
            }
            int size = chunks.size() * MAX_BUFFER_SIZE + packet.getLength() - PACKET_INFO_LENGTH;
            var buffer = ByteBuffer.allocate(size);
            for (var chunk : chunks) {
                buffer.put(chunk, PACKET_INFO_LENGTH, chunk.length - PACKET_INFO_LENGTH);
            }
            buffer.put(packet.getData(), PACKET_INFO_LENGTH, packet.getLength() - PACKET_INFO_LENGTH);
            this.forward(buffer.array());
            this.chunks.remove(sourceId);
        }
        try {
            this.inboundSocket.leaveGroup(group);
            this.inboundSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void forward(byte[] bytes) {
        try {
            var message = this.mapper.readValue(bytes, Message.class);
            if (message.destinationId == null || UuidTools.equals(message.destinationId, this.localEndpointId)) {
                this.inboundMessage.onNext(message);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
    }

    public class BytesToUUID {
        final UUID uuid;
        BytesToUUID(byte[] array, int offset) {
            long mst = ByteBuffer.allocate(Long.BYTES).put(array, offset, Long.BYTES).flip().getLong();
            long lst = ByteBuffer.allocate(Long.BYTES).put(array, offset + Long.BYTES, Long.BYTES).flip().getLong();
            this.uuid = new UUID(mst, lst);
        }
    }

    public class UUIDToBytes {
        final byte[] bytes;
        UUIDToBytes(UUID subject) {
            this.bytes = ByteBuffer.allocate(Long.BYTES * 2)
                    .putLong(subject.getMostSignificantBits())
                    .putLong(subject.getLeastSignificantBits())
                    .array();
        }
    }
}
