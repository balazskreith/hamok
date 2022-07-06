package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompositeEndpoint implements Endpoint {

    private static final Logger logger = LoggerFactory.getLogger(CompositeEndpoint.class);


    public static Builder builder() {
        return new Builder();
    }

    private String context = "No Context is given";

    private final InetRouting inetRouting = new InetRouting();
    private final Subject<Message> inbound = PublishSubject.create();
    private final Subject<Message> outbound = PublishSubject.create();
    private int multicastListenerPort = DefaultConfigs.DEFAULT_MULTICAST_PORT;
    private int unicastListenerPort = DefaultConfigs.DEFAULT_UNICAST_PORT;
    private int unicastSendingPort = DefaultConfigs.DEFAULT_UNICAST_PORT;
    private InetAddress groupAddress;
    private NetworkInterface networkInterface;
    private Codec<Message, byte[]> codec;
    private UUID endpointId;
    private BufferPool bufferPool;
    private Map<SocketOption, Object> multicastListenerOptions = Collections.emptyMap();
    private Map<SocketOption, Object> unicastListenerOptions = Collections.emptyMap();

    private AtomicReference<Process> process = new AtomicReference<>(null);

    private CompositeEndpoint() {
        this.networkInterface = DefaultConfigs.chooseDefaultInterface();
        this.bufferPool = BufferPool.createDummy(
                DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH + DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH
        );
    }

    private void init() throws SocketException {
        var packetizer = new Packetizer(this.codec);
        var socket = new DatagramSocket();
        this.outbound.subscribe(message -> {
            InetAddress address;
            int port;
            if (message.destinationId == null) {
                address = this.groupAddress;
                port = this.multicastListenerPort;
            } else {
                address = this.inetRouting.get(message.destinationId);
                port = this.unicastSendingPort;
                if (address == null) {
                    address = this.groupAddress;
                    port = this.multicastListenerPort;
                }
            }
            for (var it = packetizer.encode(message); it.hasNext(); ) {
                var bytes = it.next();
//                var packet = new DatagramPacket(bytes, bytes.length);
//                packet.setAddress(this.groupAddress);
//                packet.setPort(this.multicastListenerPort);
                var packet = new DatagramPacket(bytes, bytes.length, address, port);
                socket.send(packet);
                logger.debug("Packet sent to {}:{}", address, port);
            }

        });
    }

    @Override
    public Observable<Message> inboundChannel() {
        return this.inbound;
    }

    @Override
    public Observer<Message> outboundChannel() {
        return this.outbound;
    }

    @Override
    public void start() {
        var process = this.process.get();
        if (process != null) {
            logger.warn("Running process found ({})", this.context);
            return;
        }
        var candidate = this.createProcess();
        if (this.process.compareAndSet(null, candidate)) {
            candidate.start();
        } else {
            candidate.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return this.process.get() != null;
    }

    @Override
    public void stop() {
        var process = this.process.get();
        if (process == null) {
            logger.warn("No Running process ({})", this.context);
            return;
        }
        if (!process.isRunning()) {
            return;
        }
        process.stop();
    }

    private Process createProcess() {
        DatagramSocket unicastListener;
        MulticastSocket multicastListener;
        try {
            unicastListener = new DatagramSocket(this.unicastListenerPort);
            for (var entry : this.unicastListenerOptions.entrySet()) {
                unicastListener.setOption(entry.getKey(), entry.getValue());
            }
            unicastListener.setSoTimeout(DefaultConfigs.DEFAULT_SOCKET_RECEIVING_TIMEOUT);

            var socketAddress = new InetSocketAddress(InetAddress.getByName("0.0.0.0"), this.multicastListenerPort);
            multicastListener = new MulticastSocket(socketAddress);
            for (var entry : this.multicastListenerOptions.entrySet()) {
                multicastListener.setOption(entry.getKey(), entry.getValue());
            }
            multicastListener.setSoTimeout(DefaultConfigs.DEFAULT_SOCKET_RECEIVING_TIMEOUT);
            multicastListener.joinGroup(this.groupAddress);
        } catch (Exception e) {
            logger.error("Cannot build CompositeEndpoint ({})", this.context, e);
            return null;
        }
        var depacketizer = new Depacketizer(this.codec, this.bufferPool);
        Runnable dispose = () -> {
            try {
                multicastListener.leaveGroup(this.groupAddress);
                multicastListener.close();
            } catch (IOException e) {
                logger.warn("Exception occurred while closing multicast listener");
            }

            try {
                unicastListener.close();
            } catch (Exception e) {
                logger.warn("Exception occurred while closing uniast listener");
            }
        };

        byte[] unicastBuf = new byte[DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH + DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH];
        Runnable listenUnicast = () -> {
            DatagramPacket packet = new DatagramPacket(unicastBuf, unicastBuf.length);
            try {
                unicastListener.receive(packet);
                var message = depacketizer.decode(packet);
                if (message != null) {
                    this.inbound.onNext(message);
                }
            } catch (java.net.SocketTimeoutException e) {
                // do nothing as timeout is normal to continue
            } catch (IOException e) {
                logger.warn("Exception occurred while having IO opertion for unicast socket in listener ({})", this.context, e);
            }
        };

        byte[] multicastBuf = new byte[DefaultConfigs.DATAGRAM_PACKET_HEADER_LENGTH + DefaultConfigs.DATAGRAM_PACKET_BUFFER_MAX_LENGTH];
        Runnable listenMulticast = () -> {
            DatagramPacket packet = new DatagramPacket(multicastBuf, multicastBuf.length);
            try {
                multicastListener.receive(packet);
                var message = depacketizer.decode(packet);
                if (message == null) {
                    return;
                }
                if (message.destinationId != null && this.endpointId != null && UuidTools.notEquals(message.destinationId, this.endpointId)) {
                    return;
                }
                if (message.sourceId != null && !inetRouting.hasAddress(packet.getAddress())) {
                    inetRouting.add(message.sourceId, packet.getAddress());
                }
                this.inbound.onNext(message);
            } catch (java.net.SocketTimeoutException e) {
                // do nothing as timeout is normal to continue
            } catch (IOException e) {
                logger.warn("Exception occurred while having IO opertion for unicast socket in listener ({})", this.context, e);
            }
        };

        return Process.create(listenMulticast, listenUnicast, dispose);
    }


    public static class Builder {
        CompositeEndpoint result = new CompositeEndpoint();

        public Builder setMulticastPort(int value) {
            this.result.multicastListenerPort = value;
            return this;
        }

        public Builder setUnicastListenerPort(int value) {
            this.result.unicastListenerPort = value;
            return this;
        }

        public Builder setUnicastSendingPort(int value) {
            this.result.unicastSendingPort = value;
            return this;
        }

        public Builder setMulticastAddress(InetAddress mcastaddr) {
            this.result.groupAddress = mcastaddr;
            return this;
        }

        public Builder setCodec(Codec<Message, byte[]> codec) {
            this.result.codec = codec;
            return this;
        }

        public Builder setBufferPool(BufferPool bufferPool) {
            this.result.bufferPool = bufferPool;
            return this;
        }

        public Builder setEndpointId(UUID endpointId) {
            this.result.endpointId = endpointId;
            return this;
        }

        public Builder setContext(String context) {
            this.result.context = context;
            return this;
        }

        public<T> Builder setUnicastOption(SocketOption<T> option, T value) {
            this.result.unicastListenerOptions = Stream.concat(
                        this.result.unicastListenerOptions.entrySet().stream(),
                        Map.of(option, value).entrySet().stream()
                    ).collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
            return this;
        }

        public<T> Builder setMulticastOption(SocketOption<T> option, T value) {
            this.result.multicastListenerOptions = Stream.concat(
                    this.result.multicastListenerOptions.entrySet().stream(),
                    Map.of(option, value).entrySet().stream()
            ).collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue
            ));
            return this;
        }


        public CompositeEndpoint build() {
            try {
                this.result.init();
            } catch (SocketException e) {
                logger.warn("Exception occurred while initializing CompositeSocket ({})", this.result.context);
                return null;
            }
            return this.result;
        }
    }

    private static abstract class Process {
        static Process create(Runnable listenMulticast, Runnable listenUnicast, Runnable dispose) {
            return new Process() {
                @Override
                protected void listenMulticast() {
                    listenMulticast.run();
                }

                @Override
                protected void listenUnicast() {
                    listenUnicast.run();
                }

                @Override
                protected void disposing() {
                    dispose.run();
                }
            };
        }
        private volatile boolean run = true;
        private volatile boolean disposed = false;
        private final Thread mcastListener;
        private final Thread ucastListener;

        Process() {
            this.mcastListener = new Thread(() -> {
                while (this.run) {
                    this.listenMulticast();
                }
                this.dispose();
            });
            this.ucastListener = new Thread(() -> {
                while (this.run) {
                    this.listenUnicast();
                }
                this.dispose();
            });
        }

        public boolean isRunning() {
            return this.run;
        }

        public void start() {
            this.mcastListener.start();
            this.ucastListener.start();
        }

        public void stop() {
            this.run = false;
            try {
                this.mcastListener.join(10000);
                if (this.mcastListener.isAlive()) {
                    //  join was not successful
                    this.mcastListener.interrupt();
                }
                this.ucastListener.join(10000);
                if (this.ucastListener.isAlive()) {
                    //  join was not successful
                    this.ucastListener.interrupt();
                }
            } catch (InterruptedException e) {
                logger.warn("A thread is interrupted meanwhile it is being stopped", e);
            } finally {
                this.dispose();
            }
        }

        private void dispose() {
            if (this.disposed) {
                return;
            }
            this.disposed = true;
            this.disposing();
        }

        protected abstract void listenMulticast();
        protected abstract void listenUnicast();
        protected abstract void disposing();
    }
}
