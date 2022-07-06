package io.github.balazskreith.hamok.emulators.purgatory;

import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.transports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class UnicastEndpoint extends AbstractEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(UnicastEndpoint.class);

    public static Builder builder() {
        return new Builder();
    }

    private String context = "No Context is given";
    private int listenerPort = DefaultConfigs.DEFAULT_UNICAST_PORT;
    private int sendingPort = DefaultConfigs.DEFAULT_UNICAST_PORT;

    private InetRouting inetRouting;
    private InetAddress defaultAddress;
    private Codec<Message, byte[]> codec;
    private Packetizer packetizer;
    private DatagramSocket sender;

    private final Map<SocketOption, Object> listenerSocketOptions = new ConcurrentHashMap();
    private final Map<SocketOption, Object> senderSocketOptions = new ConcurrentHashMap();
    private final AtomicReference<DatagramSocket> listener;


    private volatile boolean run = true;

    private UnicastEndpoint() {
        this.listener = new AtomicReference<>(null);
    }

    @Override
    protected void accept(Message message) {
        InetAddress address;
        if (message.destinationId == null) {
            address = this.defaultAddress;
        } else {
            address = this.inetRouting.get(message.destinationId);
        }
        if (address == null) {
            logger.warn("Cannot send message without destination address ({}), message: {}", this.context, message);
            return;
        }
        for (var it = packetizer.encode(message); it.hasNext(); ) {
            var buffer = it.next();
            var packet = new DatagramPacket(buffer, buffer.length, address, this.sendingPort);
            logger.info("Packet send to {}:{}", packet.getAddress(), packet.getPort());
            try {
                this.sender.send(packet);
            } catch (IOException e) {
                logger.warn("Cannot sent packet. ", e);
            }
        }
    }

    @Override
    protected void run() {
        if (this.listener.get() != null) {
            logger.warn("Listener socket was expected to be null, but it was not. Has it restarted abruptly?");
            this.destroyListener();
        }
        this.createListener();
        var socket = this.listener.get();
        if (socket == null) {
            this.stop();
            logger.error("Socket is null, listening is aborted");
            return;
        }
        var depacketizer = new Depacketizer(this.codec);
        byte[] buf = new byte[DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH + DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH];
        while (this.run) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                try {
                    socket.receive(packet);
                } catch (SocketTimeoutException e) {
                    continue;
                }

                var message = depacketizer.decode(packet);
                if (message == null) {
                    continue;
                }
                this.dispatch(message);
            } catch (IOException e) {
                logger.warn("Error occurred while listening", e);
                break;
            }
        }
        this.destroyListener();
    }

    private void destroyListener() {
        var socket = this.listener.get();
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (Exception e) {
            logger.warn("Error occurred while closing listener socket ({}).", this.context, e);
        }
        this.listener.set(null);
    }

    private void createListener() {
        try {
            var socket = new DatagramSocket(this.listenerPort);
            socket.setSoTimeout(DefaultConfigs.DEFAULT_SOCKET_RECEIVING_TIMEOUT);
            for (var entry : this.listenerSocketOptions.entrySet()) {
                socket.setOption(entry.getKey(), entry.getValue());
            }
            logger.info("Socket is created ({}).", this.context);
            this.listener.set(socket);
        } catch (IOException e) {
            logger.warn("Exception occurred while creating listener socket ({})", this.context, e);
        }
    }


    public static class Builder {
        UnicastEndpoint result = new UnicastEndpoint();

        public Builder setListenerPort(int value) {
            this.result.listenerPort = value;
            return this;
        }

        public Builder setSendingPort(int value) {
            this.result.sendingPort = value;
            return this;
        }

        public Builder setCodec(Codec<Message, byte[]> codec) {
            this.result.codec = codec;
            return this;
        }

        public Builder setEndpointId(UUID endpointId) {
            this.result.setEndpointId(endpointId);
            return this;
        }

        public Builder setDefaultDestinationAddress(InetAddress address) {
            this.result.defaultAddress = address;
            return this;
        }

        public Builder setInetRouting(InetRouting routing) {
            this.result.inetRouting = routing;
            return this;
        }

        public Builder setContext(String context) {
            this.result.context = context;
            return this;
        }

        public<T> Builder setListenerOption(SocketOption<T> option, T value) {
            this.result.listenerSocketOptions.put(option, value);
            return this;
        }

        public<T> Builder setSenderOption(SocketOption<T> option, T value) {
            this.result.senderSocketOptions.put(option, value);
            return this;
        }

        public UnicastEndpoint build() {
            if (this.result.inetRouting == null) {
//                this.result.inetRouting = new InetRouting();
            }
            Objects.requireNonNull(this.result.codec, "Codec is required");
            this.result.packetizer = new Packetizer(this.result.codec);
            try {
                this.result.sender = new DatagramSocket();
            } catch (SocketException e) {
                logger.warn("Error occurred while creating socket", e);
                return null;
            }
            return this.result;
        }
    }
}
