package io.github.balazskreith.hamok.emulators;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.StorageEntry;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.storagegrid.ReplicatedStorage;
import io.github.balazskreith.hamok.storagegrid.StorageGrid;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.transports.CompositeEndpoint;
import io.github.balazskreith.hamok.transports.DefaultConfigs;
import io.github.balazskreith.hamok.transports.Endpoint;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ReplicatedStorageStandupTest {

    private InetAddress groupAddress;
    private Codec<Message, byte[]> codec;
    private StorageGrid usEastGrid;
    private StorageGrid euWestGrid;
    private Endpoint usEastEndpoint;
    private Endpoint euWestEndpoint;
    private ReplicatedStorage<Integer, Integer> usEastStorage;
    private ReplicatedStorage<Integer, Integer> euWestStorage;

    public ReplicatedStorageStandupTest() throws UnknownHostException {
        this.groupAddress = InetAddress.getByName("225.1.2.1");
        var mapper = new ObjectMapper();
        this.codec = Codec.<Message, byte[]>create(
                Mapper.create(message -> mapper.writeValueAsBytes(message)),
                Mapper.create(bytes -> {
                    if (1000 < bytes.length) {
                        var str = new String(bytes);
//                        System.out.println(str);
                    }
                    try {
                        var mismatched = mapper.readValue(bytes, Message.class);
                        return mismatched;
                    } catch (Exception e) {
                        return null;
                    }
                })
        );
    }

    @Test
    @Order(1)
    @DisplayName("Setup US East")
    public void setup() throws IOException, InterruptedException {
        this.usEastGrid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(30000)
                .withContext("US East")
                .withAutoDiscovery(true)
                .build();

        this.usEastEndpoint = CompositeEndpoint.builder()
                .setMulticastPort(DefaultConfigs.DEFAULT_MULTICAST_PORT)
                .setUnicastListenerPort(5600)
                .setUnicastSendingPort(5601)
                .setEndpointId(this.usEastGrid.getLocalEndpointId())
                .setCodec(codec)
                .setMulticastAddress(this.groupAddress)
                .setContext("US East Endpoint")
                .setMulticastOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setMulticastOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
                .setUnicastOption(StandardSocketOptions.SO_BROADCAST, true)
                .build();

        var intCodec = Codec.<Integer, byte[]>create(i -> ByteBuffer.allocate(4).putInt(i).array(), arr -> ByteBuffer.wrap(arr).getInt());
        this.usEastStorage = this.usEastGrid.<Integer, Integer>replicatedStorage()
                .setStorageId("myStorage")
                .setKeyCodecSupplier(() -> intCodec)
                .setValueCodecSupplier(() ->  intCodec)
                .setMaxMessageKeys(100000)
                .setMaxMessageValues(100000)
                .build();

        var entries = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100000; ++i) {
            int key = getRandomNumber();
            int value = getRandomNumber();
            entries.put(key, value);
        }

        this.usEastStorage.insertAll(entries);

    }

    @Test
    @Order(2)
    @DisplayName("Setup EU West")
    void replicatedStorageTest() throws InterruptedException {

        this.euWestGrid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(30000)
                .withContext("EU West")
                .withAutoDiscovery(true)
                .build();

        this.euWestEndpoint = CompositeEndpoint.builder()
                .setMulticastPort(DefaultConfigs.DEFAULT_MULTICAST_PORT)
                .setUnicastListenerPort(5601)
                .setUnicastSendingPort(5600)
                .setEndpointId(this.euWestGrid.getLocalEndpointId())
                .setCodec(codec)
                .setMulticastAddress(this.groupAddress)
                .setContext("EU West Endpoint")
                .setMulticastOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setMulticastOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
                .setUnicastOption(StandardSocketOptions.SO_BROADCAST, true)
                .build();

        var intCodec = Codec.<Integer, byte[]>create(i -> ByteBuffer.allocate(4).putInt(i).array(), arr -> ByteBuffer.wrap(arr).getInt());
        this.euWestStorage = this.euWestGrid.<Integer, Integer>replicatedStorage()
                .setStorageId("myStorage")
                .setKeyCodecSupplier(() -> intCodec)
                .setValueCodecSupplier(() -> intCodec)
                .setMaxMessageKeys(100000)
                .setMaxMessageValues(100000)
                .build();

        var countdown = new CountDownLatch(2 + this.usEastStorage.localSize());
        this.euWestGrid.joinedRemoteEndpoints().subscribe(e -> countdown.countDown());
        this.usEastGrid.joinedRemoteEndpoints().subscribe(e -> countdown.countDown());
        this.euWestStorage.events().createdEntry().subscribe(e -> countdown.countDown());

        this.usEastGrid.transport().getSender().subscribe(this.usEastEndpoint.outboundChannel());
        this.usEastEndpoint.inboundChannel().subscribe(this.usEastGrid.transport().getReceiver());

        this.euWestGrid.transport().getSender().subscribe(this.euWestEndpoint.outboundChannel());
        this.euWestEndpoint.inboundChannel().subscribe(this.euWestGrid.transport().getReceiver());

        this.usEastEndpoint.start();
        this.euWestEndpoint.start();

        if (!countdown.await(50000, TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Timeout");
        }

        try {
            Iterable<StorageEntry<Integer, Integer>> iterable = () -> this.usEastStorage.localIterator();
            for (var entry : iterable) {
                var key = entry.getKey();
                var expected = entry.getValue();
                var actual = euWestStorage.get(key);
                Assertions.assertEquals(expected, actual);
            }
        } finally {
            usEastEndpoint.stop();
            euWestEndpoint.stop();
        }
    }

    private static Integer getRandomNumber() {
        return (int) (Math.random() * (Integer.MAX_VALUE - 1));
    }
}
