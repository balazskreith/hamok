package io.github.balazskreith.hamok.emulators;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.storagegrid.FederatedStorage;
import io.github.balazskreith.hamok.storagegrid.StorageGrid;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.transports.CompositeEndpoint;
import io.github.balazskreith.hamok.transports.DefaultConfigs;
import io.github.balazskreith.hamok.transports.Endpoint;
import org.junit.jupiter.api.*;

import java.net.InetAddress;
import java.net.StandardSocketOptions;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class FederatedStorageWorkingTest {

    private static StorageGrid usEastGrid;
    private static StorageGrid euWestGrid;
    private static Endpoint usEastEndpoint;
    private static Endpoint euWestEndpoint;
    private FederatedStorage<Integer, Integer> usEastStorage;
    private FederatedStorage<Integer, Integer> euWestStorage;

    @BeforeEach
    private void init() {
        var storageId = UUID.randomUUID().toString();
        var intCodec = Codec.<Integer, byte[]>create(i -> ByteBuffer.allocate(4).putInt(i).array(), arr -> ByteBuffer.wrap(arr).getInt());
        usEastStorage = usEastGrid.<Integer, Integer>federatedStorage()
                .setStorageId(storageId)
                .setKeyCodecSupplier(() -> intCodec)
                .setValueCodecSupplier(() -> intCodec)
                .setMergeOperator(() -> (v1, v2) -> v1 + v2)
                .setMaxMessageKeys(10000)
                .setMaxMessageValues(10000)
                .build();


        euWestStorage = euWestGrid.<Integer, Integer>federatedStorage()
                .setStorageId(storageId)
                .setKeyCodecSupplier(() -> intCodec)
                .setValueCodecSupplier(() -> intCodec)
                .setMergeOperator(() -> (v1, v2) -> v1 + v2)
                .setMaxMessageKeys(10000)
                .setMaxMessageValues(10000)
                .build();
    }

    @AfterEach
    private void deinit() throws Exception {
        usEastStorage.close();
        euWestStorage.close();
    }

    @BeforeAll
    static void setup() throws UnknownHostException, InterruptedException {
        usEastGrid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(300000)
                .withContext("US East")
                .withAutoDiscovery(true)
                .build();

        euWestGrid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(300000)
                .withContext("EU West")
                .withAutoDiscovery(true)
                .build();

//        var netif = chooseDefaultInterface();
//        System.out.println(netif.toString());
        var group = InetAddress.getByName("225.1.2.1");
//        var group = InetAddress.getByName("FF02::1");
        var mapper = new ObjectMapper();
        var codec = Codec.<Message, byte[]>create(
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

        usEastEndpoint = CompositeEndpoint.builder()
                .setMulticastPort(DefaultConfigs.DEFAULT_MULTICAST_PORT)
                .setUnicastListenerPort(5600)
                .setUnicastSendingPort(5601)
                .setEndpointId(usEastGrid.getLocalEndpointId())
                .setCodec(codec)
                .setMulticastAddress(group)
                .setContext("US East Endpoint")
                .setMulticastOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setMulticastOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
                .setUnicastOption(StandardSocketOptions.SO_BROADCAST, true)
                .build();

        euWestEndpoint = CompositeEndpoint.builder()
                .setMulticastPort(DefaultConfigs.DEFAULT_MULTICAST_PORT)
                .setUnicastListenerPort(5601)
                .setUnicastSendingPort(5600)
                .setEndpointId(euWestGrid.getLocalEndpointId())
                .setCodec(codec)
                .setMulticastAddress(group)
                .setContext("EU West Endpoint")
                .setMulticastOption(StandardSocketOptions.SO_REUSEADDR, true)
                .setMulticastOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
                .setUnicastOption(StandardSocketOptions.SO_BROADCAST, true)
                .build();

        var countdown = new CountDownLatch(2);
        usEastGrid.joinedRemoteEndpoints().subscribe(e -> countdown.countDown());
        euWestGrid.joinedRemoteEndpoints().subscribe(e -> countdown.countDown());

        usEastGrid.transport().getSender().subscribe(usEastEndpoint.outboundChannel());
        usEastEndpoint.inboundChannel().subscribe(usEastGrid.transport().getReceiver());

        euWestGrid.transport().getSender().subscribe(euWestEndpoint.outboundChannel());
        euWestEndpoint.inboundChannel().subscribe(euWestGrid.transport().getReceiver());

        usEastEndpoint.start();
        euWestEndpoint.start();

        countdown.await(50000, TimeUnit.MILLISECONDS);
    }

    @AfterAll
    static void teardown() {
        usEastEndpoint.stop();
        euWestEndpoint.stop();
    }

    @Test
    @DisplayName("When entries are inserted to one separated storage it can be accessed to another one")
    void insertAll() throws InterruptedException {
        var keys = new HashSet<Integer>();
        var usEastEntries = new HashMap<Integer, Integer>();
        var euWestEntries = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100000; ++i) {
            int key = getRandomNumber() % 1000000;
            keys.add(key);
            usEastEntries.put(key, getRandomNumber() % 100);
            euWestEntries.put(key, getRandomNumber() % 100);
        }

        usEastStorage.insertAll(usEastEntries);
        euWestStorage.insertAll(euWestEntries);
        Assertions.assertEquals(euWestEntries.size(), euWestStorage.localSize());
        Assertions.assertEquals(usEastEntries.size(), usEastStorage.localSize());

        // let's check if chunking is also working
        var usEastExtractedEntries = usEastStorage.getAll(keys);
        var euWestExtractedEntries = euWestStorage.getAll(keys);
        for (var key : keys) {
            var expected = usEastEntries.get(key) + euWestEntries.get(key);
            var euWestActual = euWestStorage.get(key);
            var usEastActual = usEastStorage.get(key);
            Assertions.assertEquals(expected, euWestActual);
            Assertions.assertEquals(expected, usEastActual);
            Assertions.assertEquals(expected, usEastExtractedEntries.get(key));
            Assertions.assertEquals(expected, euWestExtractedEntries.get(key));
        }
    }

    @Test
    @DisplayName("When entries are updated to one separated storage it is updated on all")
    void updateAll() throws InterruptedException {
        var keys = new HashSet<Integer>();
        var usEastEntries = new HashMap<Integer, Integer>();
        var euWestEntries = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100000; ++i) {
            int key = getRandomNumber();
            keys.add(key);
            usEastEntries.put(key, getRandomNumber());
            euWestEntries.put(key, getRandomNumber());
        }

        // wait for insert
        usEastStorage.insertAll(usEastEntries);
        euWestStorage.insertAll(euWestEntries);
        Assertions.assertEquals(euWestEntries.size(), euWestStorage.localSize());
        Assertions.assertEquals(usEastEntries.size(), usEastStorage.localSize());

        for (var key : euWestEntries.keySet().stream().collect(Collectors.toList())) {
            var newValue = euWestEntries.get(key) + 1;
            euWestEntries.put(key, newValue);
        }
        euWestStorage.setAll(euWestEntries);
        for (var key : usEastEntries.keySet().stream().collect(Collectors.toList())) {
            var newValue = usEastEntries.get(key) + 1;
            usEastEntries.put(key, newValue);
        }
        usEastStorage.setAll(usEastEntries);

        for (var key : keys) {
            var expected = usEastEntries.get(key) + euWestEntries.get(key);
            var euWestActual = euWestStorage.get(key);
            var usEastActual = usEastStorage.get(key);
            Assertions.assertEquals(expected, euWestActual);
            Assertions.assertEquals(expected, usEastActual);
        }
    }


    @Test
    @DisplayName("When entries are deleted to one separated storage it is deleted on another one")
    void deleteAll() throws InterruptedException {
        var keys = new HashSet<Integer>();
        var usEastEntries = new HashMap<Integer, Integer>();
        var euWestEntries = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100000; ++i) {
            int key = getRandomNumber();
            keys.add(key);
            usEastEntries.put(key, getRandomNumber());
            euWestEntries.put(key, getRandomNumber());
        }

        usEastStorage.setAll(usEastEntries);
        euWestStorage.setAll(euWestEntries);
        Assertions.assertEquals(euWestEntries.size(), euWestStorage.localSize());
        Assertions.assertEquals(usEastEntries.size(), usEastStorage.localSize());

        usEastStorage.deleteAll(euWestEntries.keySet());
        euWestStorage.deleteAll(usEastEntries.keySet());

        for (var key : keys) {
            var expected = usEastEntries.get(key) + euWestEntries.get(key);
            var euWestActual = euWestStorage.get(key);
            var usEastActual = usEastStorage.get(key);
            Assertions.assertNull(euWestActual);
            Assertions.assertNull(usEastActual);
        }
    }

    private static Integer getRandomNumber() {
        return (int) (Math.random() * (Integer.MAX_VALUE - 1));
    }
}
