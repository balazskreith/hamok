package io.github.balazskreith.hamok.emulators;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.storagegrid.StorageGrid;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.transports.AbstractEndpoint;
import io.github.balazskreith.hamok.transports.MulticastEndpoint;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.StandardSocketOptions;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class Emulator2 {

    private StorageGrid usEastGrid;
    private StorageGrid euWestGrid;
    private AbstractEndpoint usEastEndpoint;
    private AbstractEndpoint euWestEndpoint;

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
        var mapper = new ObjectMapper();
        var codec = Codec.<Message, byte[]>create(
                Mapper.create(message -> mapper.writeValueAsBytes(message)),
                Mapper.create(bytes -> mapper.readValue(bytes, Message.class))
        );

        var usEastEndpoint = MulticastEndpoint.builder()
                .setCodec(codec)
                .setAddress(group)
                .setEndpointId(this.usEastGrid.getLocalEndpointId())
                .setContext("US East Endpoint")
                .setListenerOption(StandardSocketOptions.SO_REUSEADDR, true)
                .build();

        var euWestEndpoint = MulticastEndpoint.builder()
                .setCodec(codec)
                .setAddress(group)
                .setEndpointId(this.euWestGrid.getLocalEndpointId())
                .setContext("EU West Endpoint")
                .setListenerOption(StandardSocketOptions.SO_REUSEADDR, true)
                .build();
        try {
            usEastEndpoint.start();
            euWestEndpoint.start();

            this.usEastGrid.transport().getSender().subscribe(usEastEndpoint);
            usEastEndpoint.subscribe(this.usEastGrid.transport().getReceiver());

            this.euWestGrid.transport().getSender().subscribe(euWestEndpoint);
            euWestEndpoint.subscribe(this.euWestGrid.transport().getReceiver());
            Thread.sleep(50000);
        } finally {
            usEastEndpoint.stop();
            euWestEndpoint.stop();
        }
    }
}
