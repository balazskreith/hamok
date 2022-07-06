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

public class MulticastEndpoint extends AbstractEndpoint {

    private static final Logger logger = LoggerFactory.getLogger(MulticastEndpoint.class);

    public static Builder builder() {
        return new Builder();
    }

    private String context = "No Context is given";
    private int port = DefaultConfigs.DEFAULT_MULTICAST_PORT;
    private InetRouting inetRouting;
    private NetworkInterface netIf;
    private Codec<Message, byte[]> codec;
    private Packetizer packetizer;
    private InetAddress groupAddress;
    private DatagramSocket sender;

    private final Map<SocketOption, Object> listenerSocketOptions = new ConcurrentHashMap();
    private final Map<SocketOption, Object> senderSocketOptions = new ConcurrentHashMap();
    private final AtomicReference<MulticastSocket> listener;


    private volatile boolean run = true;

    private MulticastEndpoint() {
        this.listener = new AtomicReference<>(null);
    }

    @Override
    protected void accept(Message message) {
        InetAddress address = this.groupAddress;
        for (var it = packetizer.encode(message); it.hasNext(); ) {
            var buffer = it.next();
            var packet = new DatagramPacket(buffer, buffer.length, address, this.port);
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
        var socket = this.createListener();
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
                } catch (java.net.SocketTimeoutException e) {
                    continue;
                }
                if (this.inetRouting != null && !this.inetRouting.hasAddress(packet.getAddress())) {
                    this.inetRouting.add(this.getEndpointId(), packet.getAddress());
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
            if (this.netIf != null) {
                var socketAddress = new InetSocketAddress(this.groupAddress, this.port);
                socket.leaveGroup(socketAddress, this.netIf);
            } else {
                socket.leaveGroup(this.groupAddress);
            }
        } catch (IOException e) {
            logger.warn("Error occurred while trying to leave group {} for listener socket ({}).", this.groupAddress.toString(), this.context, e);
        }
        this.listener.set(null);
    }

    private MulticastSocket createListener() {
        try {
            var socket = new MulticastSocket(this.port);
            socket.setSoTimeout(DefaultConfigs.DEFAULT_SOCKET_RECEIVING_TIMEOUT);
//            socket.setTimeToLive(1);
            for (var entry : this.listenerSocketOptions.entrySet()) {
                socket.setOption(entry.getKey(), entry.getValue());
            }
            if (this.netIf != null) {
                var socketAddress = new InetSocketAddress(this.groupAddress, this.port);
                socket.joinGroup(socketAddress, this.netIf);
            } else {
                logger.warn("Joining to a multicast group without a given interface is deprecated in java since v14. please consider providing the interface");
                socket.joinGroup(this.groupAddress);
            }
            logger.info("Socket is created ({}). multicast: {}", this.context, this.groupAddress != null && this.groupAddress.isMulticastAddress());
            this.listener.set(socket);
            return socket;
        } catch (IOException e) {
            logger.warn("Exception occurred while creating listener socket ({})", this.context, e);
            return null;
        }
    }


    public static class Builder {
        MulticastEndpoint result = new MulticastEndpoint();

        public Builder setPort(int value) {
            this.result.port = value;
            return this;
        }

        public Builder setAddress(InetAddress mcastaddr) {
            this.result.groupAddress = mcastaddr;
            return this;
        }

        public Builder setCodec(Codec<Message, byte[]> codec) {
            this.result.codec = codec;
            return this;
        }

        Builder setInetRouting(InetRouting inetRouting) {
            this.result.inetRouting = inetRouting;
            return this;
        }

        public Builder setEndpointId(UUID endpointId) {
            this.result.setEndpointId(endpointId);
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

        public Builder setNetworkInterface(NetworkInterface netIf) {
            this.result.netIf = netIf;
            return this;
        }

        public MulticastEndpoint build() {
            Objects.requireNonNull(this.result.groupAddress, "address is required");
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
