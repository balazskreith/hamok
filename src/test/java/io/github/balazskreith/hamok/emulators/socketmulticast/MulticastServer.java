//package io.github.balazskreith.hamok.emulators.socketmulticast;
//
//import io.github.balazskreith.hamok.mappings.Codec;
//import io.github.balazskreith.hamok.storagegrid.messages.Message;
//import io.github.balazskreith.hamok.transports.Depacketizer;
//import io.github.balazskreith.hamok.transports.AbstractEndpoint;
//import io.github.balazskreith.hamok.transports.Packetizer;
//import io.netty.bootstrap.Bootstrap;
//import io.netty.bootstrap.ChannelFactory;
//import io.netty.buffer.ByteBufUtil;
//import io.netty.channel.*;
//import io.netty.channel.nio.NioEventLoopGroup;
//import io.netty.channel.socket.DatagramPacket;
//import io.netty.channel.socket.InternetProtocolFamily;
//import io.netty.channel.socket.nio.NioDatagramChannel;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.net.*;
//import java.util.Enumeration;
//
//public class MulticastServer extends AbstractEndpoint {
//
//    private static final Logger logger = LoggerFactory.getLogger(MulticastServer.class);
//
//    private final Packetizer packetizer;
//    private final Depacketizer depacketizer;
//    private InetSocketAddress groupAddress;
//
//    public MulticastServer(InetSocketAddress groupAddress, Codec<Message, byte[]> codec) {
//        this.groupAddress = groupAddress;
//        this.packetizer = new Packetizer(codec);
//        this.depacketizer = new Depacketizer(codec);
//        try {
//            var socket = new DatagramSocket();
//            this.outbound().subscribe(message -> {
//                InetAddress address = null; // this.inetRouting.get(message.sourceId);
//                if (address == null) {
//                    address = this.groupAddress.getAddress();
//                }
//                for (var it = packetizer.encode(message); it.hasNext(); ) {
//                    var buffer = it.next();
//                    var packet = new java.net.DatagramPacket(buffer, buffer.length);
//                    packet.setPort(groupAddress.getPort());
//                    packet.setAddress(address);
//                    logger.info("Packet send to {}:{}", packet.getAddress(), packet.getPort());
//                    try {
//                        socket.send(packet);
//                    } catch (IOException e) {
//                        logger.warn("Cannot sent packet. ", e);
//                    }
//                }
//            });
//        } catch (SocketException e) {
//            logger.warn("Socket exception", e);
//        }
//    }
//
//
//    private class MulticastHandler extends  SimpleChannelInboundHandler<DatagramPacket> {
//        @Override
//        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
////            var bytes = msg.content().array();
//            var bytes = ByteBufUtil.getBytes(msg.content());
//            var packet = new java.net.DatagramPacket(bytes, bytes.length);
//            packet.setAddress(msg.sender().getAddress());
//            var message = depacketizer.decode(packet);
//            if (message != null) {
//                inbound().onNext(message);
//            }
//            System.out.println("receive");
//        }
//    }
//
//    @Override
//    protected void run() {
//        EventLoopGroup group = new NioEventLoopGroup();
//        try {
//            NetworkInterface ni = NetworkInterface.getByName("en0");
//            Enumeration<InetAddress> addresses = ni.getInetAddresses();
//            InetAddress localAddress = null;
//            while (addresses.hasMoreElements()) {
//                InetAddress address = addresses.nextElement();
//                if (address instanceof Inet4Address){
//                    localAddress = address;
//                }
//            }
//
//            Bootstrap b = new Bootstrap()
//                    .group(group)
//                    .channelFactory(new ChannelFactory<NioDatagramChannel>() {
//                        @Override
//                        public NioDatagramChannel newChannel() {
//                            return new NioDatagramChannel(InternetProtocolFamily.IPv4);
//                        }
//                    })
//                    .localAddress(localAddress, groupAddress.getPort())
//                    .option(ChannelOption.IP_MULTICAST_IF, ni)
//                    .option(ChannelOption.SO_REUSEADDR, true)
//                    .handler(new ChannelInitializer<NioDatagramChannel>() {
//                        @Override
//                        public void initChannel(NioDatagramChannel ch) throws Exception {
//                            ch.pipeline().addLast(new MulticastHandler());
//                        }
//                    });
//
//            NioDatagramChannel ch = (NioDatagramChannel)b.bind(groupAddress.getPort()).sync().channel();
//            ch.joinGroup(groupAddress, ni).sync();
//            ch.closeFuture().await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (SocketException e) {
//            e.printStackTrace();
//        } finally {
//            group.shutdownGracefully();
//        }
//    }
//}