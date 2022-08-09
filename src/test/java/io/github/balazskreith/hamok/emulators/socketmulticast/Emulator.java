package io.github.balazskreith.hamok.emulators.socketmulticast;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.storagegrid.StorageGrid;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

@DisplayName("Federation Storage Stress Test Scenario. While federated storages are distributed through the grid, endpoint can be joined and detached, but the storage remains consequent.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Emulator {

    private StorageGrid usEastGrid;
    private StorageGrid euWestGrid;
    private PurgatoryMulticastEndpoint usEastEndpoint;
    private PurgatoryMulticastEndpoint euWestEndpoint;

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

        var group = InetAddress.getByName("230.0.0.3");
        var groupAddress = new InetSocketAddress(group, 12345);
        var mapper = new ObjectMapper();
        var codec = Codec.<Message, byte[]>create(
                Mapper.create(message -> mapper.writeValueAsBytes(message)),
                Mapper.create(bytes -> mapper.readValue(bytes, Message.class))
        );
//        var endpoint = new MulticastServer(groupAddress, codec);
//        endpoint.start();
////        this.usEastEndpoint = new PurgatoryMulticastEndpoint(group, 1243, this.usEastGrid.getLocalEndpointId());
////        this.euWestEndpoint = new PurgatoryMulticastEndpoint(group, 1243, this.euWestGrid.getLocalEndpointId());
//
//        this.usEastGrid.transport().getSender().subscribe(endpoint);
//        endpoint.subscribe(this.usEastGrid.transport().getReceiver());
//
//        this.euWestGrid.transport().getSender().subscribe(endpoint);
//        endpoint.subscribe(this.euWestGrid.transport().getReceiver());

        Thread.sleep(20000);
    }
}
