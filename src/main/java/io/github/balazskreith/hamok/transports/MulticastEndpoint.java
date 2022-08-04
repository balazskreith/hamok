package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Objects;
import java.util.UUID;

public class MulticastEndpoint extends Endpoint {

    private static final Logger logger = LoggerFactory.getLogger(MulticastEndpoint.class);

    public static Builder builder() {
        return new Builder();
    }

    private InetRouting inetRouting;
    private Config config;
    private NetworkInterface netIf;
    private Codec<Message, byte[]> codec;
    private InetAddress address;
    private InetSocketAddress group;

    private volatile boolean run = true;

    private MulticastEndpoint() {
    }

    @Override
    protected void process() {
        MulticastSocket multicastSocket;
        try {
            multicastSocket = new MulticastSocket(this.config.port);
            if (this.netIf != null) {
                multicastSocket.joinGroup(this.group, netIf);
            } else {
                logger.warn("Joining to a multicast group without specifying network interface is deprecated. Please consider to provide that info");
                multicastSocket.joinGroup(this.address);
            }
            multicastSocket.connect(this.group.getAddress(), this.config.port);
            logger.warn("isConnected: {}", multicastSocket.isConnected());
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
                    multicastSocket.send(packet);
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
                multicastSocket.receive(packet);
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
        final MulticastEndpoint result = new MulticastEndpoint();
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

        public Builder setNetworkInterface(NetworkInterface netIf) {
            this.result.netIf = netIf;
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

        public MulticastEndpoint build() {
            if (this.result.inetRouting == null) {
                this.result.inetRouting = new InetRouting();
            }
            Objects.requireNonNull(this.result.config, "Config is required");
            Objects.requireNonNull(this.result.address, "address is required");
            Objects.requireNonNull(this.result.codec, "Codec is required");
            this.result.group = new InetSocketAddress(this.result.address, this.result.config.port);
            return this.result;
        }
    }
}
