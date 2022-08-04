package io.github.balazskreith.hamok.emulators.socketmulticast;

import io.github.balazskreith.hamok.storagegrid.StorageGrid;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetAddress;

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
        this.usEastEndpoint = new PurgatoryMulticastEndpoint(group, 1243, this.usEastGrid.getLocalEndpointId());
        this.euWestEndpoint = new PurgatoryMulticastEndpoint(group, 1243, this.euWestGrid.getLocalEndpointId());

        this.usEastGrid.transport().getSender().subscribe(this.usEastEndpoint.sender());
        this.usEastEndpoint.receiver().subscribe(this.usEastGrid.transport().getReceiver());

        this.euWestGrid.transport().getSender().subscribe(this.euWestEndpoint.sender());
        this.euWestEndpoint.receiver().subscribe(this.euWestGrid.transport().getReceiver());

        Thread.sleep(20000);
    }
}
