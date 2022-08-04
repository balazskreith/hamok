package io.github.balazskreith.hamok.emulators;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.storagegrid.StorageGrid;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.transports.Endpoint;
import io.github.balazskreith.hamok.transports.MulticastEndpoint;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Emulator2 {

    private StorageGrid usEastGrid;
    private StorageGrid euWestGrid;
    private Endpoint usEastEndpoint;
    private Endpoint euWestEndpoint;

    @Test
    @Order(1)
    @DisplayName("Setup")
    public void setup() throws IOException, InterruptedException {
        this.usEastGrid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(30000)
                .withContext("US East")
                .withAutoDiscovery(true)
                .build();

        this.euWestGrid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(30000)
                .withContext("EU West")
                .withAutoDiscovery(true)
                .build();

        var group = InetAddress.getByName("225.1.1.1");
//        var group = InetAddress.getByName("127.0.0.1");
        var mapper = new ObjectMapper();
        var codec = Codec.<Message, byte[]>create(
                Mapper.create(message -> mapper.writeValueAsBytes(message)),
                Mapper.create(bytes -> mapper.readValue(bytes, Message.class))
        );
        var config = new MulticastEndpoint.Config(1400, 5432);
        var netif = NetworkInterface.getByName("en0");
        this.usEastEndpoint = MulticastEndpoint.builder()
                .setCodec(codec)
                .setAddress(group)
                .setConfig(config)
//                .setNetworkInterface(netif)
                .build();
//        this.euWestEndpoint = MulticastEndpoint.builder()
//                .setCodec(codec)
//                .setAddress(group)
//                .setConfig(config)
////                .setNetworkInterface(netif)
//                .build();

        this.usEastEndpoint.start();
//        this.euWestEndpoint.start();

        this.usEastGrid.transport().getSender().subscribe(this.usEastEndpoint);
        this.usEastEndpoint.subscribe(this.usEastGrid.transport().getReceiver());

        this.euWestGrid.transport().getSender().subscribe(this.usEastEndpoint);
        this.usEastEndpoint.subscribe(this.euWestGrid.transport().getReceiver());

        Thread.sleep(20000);
    }
}
