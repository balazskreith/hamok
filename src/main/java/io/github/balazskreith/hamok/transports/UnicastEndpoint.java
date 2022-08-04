package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Objects;
import java.util.UUID;

public class UnicastEndpoint extends Endpoint {

    private static final Logger logger = LoggerFactory.getLogger(UnicastEndpoint.class);

    public static Builder builder() {
        return new Builder();
    }

    private InetRouting inetRouting;
    private Config config;
    private Codec<Message, byte[]> codec;
    private InetAddress address;

    private volatile boolean run = true;

    private UnicastEndpoint() {
    }

    @Override
    protected void process() {
        DatagramSocket socket;
        try {
            socket = new DatagramSocket(this.config.port);
            socket.connect(this.address, this.config.port);
            logger.info("Socket connected: {}", socket.isConnected());
        } catch (IOException e) {
            logger.warn("Exception occurred while opening socket", e);
            return;
        }
        var packetizer = new Packetizer(this.codec);
        this.outbound().subscribe(message -> {
            InetAddress address = this.inetRouting.get(message.sourceId);
            if (address == null) {
                address = this.address;
            }
            for (var it = packetizer.encode(message); it.hasNext(); ) {
                var buffer = it.next();
                var packet = new DatagramPacket(buffer, buffer.length, address, this.config.port);
                logger.info("Packet send to {}:{}", packet.getAddress(), packet.getPort());
                try {
                    socket.send(packet);
                } catch (IOException e) {
                    logger.warn("Cannot sent packet. ", e);
                }
            }
        });
        var depacketizer = new Depacketizer(this.codec);
        byte[] buf = new byte[this.config.maxPacketLength];
        while (this.run) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            try {
                socket.receive(packet);
                var message = depacketizer.decode(packet);
                if (message == null) {
                    continue;
                }
                if (message.sourceId != null && this.inetRouting.get(message.sourceId) == null) {
                    this.inetRouting.add(message.sourceId, packet.getAddress());
                }
                this.inbound().onNext(message);
            } catch (IOException e) {
                logger.warn("Error occurred while listening", e);
                break;
            }
        }
    }

    public record Config(int maxPacketLength, int port) {

    }

    public static class Builder {
        final UnicastEndpoint result = new UnicastEndpoint();
        // Config config, InetRouting routing, InetAddress mcastaddr, NetworkInterface netIf, Decoder<byte[], Message> decoder
        public Builder setConfig(Config config) {
            this.result.config = config;
            return this;
        }

        public Builder setRouting(InetRouting routing) {
            this.result.inetRouting = routing;
            return this;
        }

        public Builder setAddress(InetAddress mcastaddr) {
            this.result.address = mcastaddr;
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

        public UnicastEndpoint build() {
            if (this.result.inetRouting == null) {
                this.result.inetRouting = new InetRouting();
            }
            Objects.requireNonNull(this.result.config, "Config is required");
            Objects.requireNonNull(this.result.address, "address is required");
            Objects.requireNonNull(this.result.codec, "Codec is required");
            return this.result;
        }
    }
}
