package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;

@DisplayName("Federation Storage Stress Test Scenario. While federated storages are distributed through the grid, endpoint can be joined and detached, but the storage remains consequent.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageWorkingTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageWorkingTest.class);

//    private static final int EXPIRATION_TIME_IN_MS = 5000;
    private static final int EXPIRATION_TIME_IN_MS = 15000;
    private static final int GOLD_STOCKPILE_VALUE = 2;
    private static final int SILVER_STOCKPILE_VALUE = 3;
    private static final int BRONZE_STOCKPILE_VALUE = 5;
    private static final String GOLD_STOCKPILE_KEY = "gold";
    private static final String SILVER_STOCKPILE_KEY = "silver";
    private static final String BRONZE_STOCKPILE_KEY = "bronze";
    private static final String STORAGE_ID = "Stockpiles";


    private StorageGrid euWest;
    private StorageGrid usEast;
    private StorageGrid asEast;
    private ReplicatedStorage<String, Integer> bcnStockpile;
    private ReplicatedStorage<String, Integer> nyStockpile;
    private ReplicatedStorage<String, Integer> hkStockpile;
    private StorageGridRouter router = new StorageGridRouter(Codec.create(JsonUtils::objectToBytes, bytes -> JsonUtils.bytesToObject(bytes, Message.class)));

    @Test
    @Order(1)
    @DisplayName("When euWest, and usEast are up and bcnStockpile, nyStockpile are created Then they are empty")
    void test_1() {
        this.euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
                .build();
        this.usEast = StorageGrid.builder()
                .withContext("US east")
                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
                .build();

        var keyCodec = Codec.<String, String>create(str -> str, str -> str);
        var valueCodec = Codec.<Integer, String>create(i -> i.toString(), str -> Integer.parseInt(str));
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        bcnStockpile = euWest.<String, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        nyStockpile = usEast.<String, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        Assertions.assertTrue(bcnStockpile.isEmpty());
        Assertions.assertTrue(nyStockpile.isEmpty());
        Assertions.assertEquals(0, bcnStockpile.size());
        Assertions.assertEquals(0, nyStockpile.size());
    }

    @Test
    @Order(2)
    @DisplayName("When euWest and usEast are not connected Then they store their entries in their local storage")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        bcnStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE);
        nyStockpile.set(SILVER_STOCKPILE_KEY, SILVER_STOCKPILE_VALUE);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, bcnStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, nyStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertNull(bcnStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertNull(nyStockpile.get(GOLD_STOCKPILE_KEY));
    }

    @Test
    @Order(3)
    @DisplayName("When euWest and usEast are connected Then they elect a leader and that leader id should be the same")
    void test_3() throws ExecutionException, InterruptedException, TimeoutException {
        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();

        euWest.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(euWestIsReady::complete);
        usEast.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(usEastIsReady::complete);

        this.router.add(euWest.getLocalEndpointId(), euWest.transport());
        this.router.add(usEast.getLocalEndpointId(), usEast.transport());

        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(euWestIsReady.get(), usEastIsReady.get());

        // because of the initial insert?
        Thread.sleep(5000);
    }

    @Test
    @Order(4)
    @DisplayName("When bcnStockpile have value for gold, and nyStockpile have value for silver Then nyStockpile can get gold, bcnStockpile can get value for silver")
    void test_4() throws InterruptedException {
        bcnStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE);
        nyStockpile.set(SILVER_STOCKPILE_KEY, SILVER_STOCKPILE_VALUE);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, nyStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, bcnStockpile.get(SILVER_STOCKPILE_KEY));

        Thread.sleep(1000);

        Assertions.assertFalse(bcnStockpile.isEmpty());
        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertEquals(2, bcnStockpile.size());
        Assertions.assertEquals(2, nyStockpile.size());

        Assertions.assertEquals(2, bcnStockpile.localSize());
        Assertions.assertEquals(2, nyStockpile.localSize());
    }

    /**
     *
     *
     */
    @Test
    @Order(5)
    @DisplayName("When the amount of gold is modified in bcnStockpile then nyStockpile will get the updated value")
    void test_5() throws InterruptedException {
        bcnStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE + 1);
        nyStockpile.set(SILVER_STOCKPILE_KEY, SILVER_STOCKPILE_VALUE + 1);

        // NOTE: we need to modify both storage, because if we would only modify one and that one is
        // the leader then the response of being modified is going to be faster than the other
        // executed the commit and the other will not execute the commit on the query thread (so the testing thread)
        // at the time it queries. this is not a bug! (Neither a feature), it is inherited from Raft!
        // Raft spread entries accross the cluster and the leader commits first before other got the
        // commit index. It is absolutely sure that other will execute the same, but not on the same thread.
        Thread.sleep(1000);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE + 1, nyStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE + 1, bcnStockpile.get(SILVER_STOCKPILE_KEY));

    }

    @Test
    @Order(6)
    @DisplayName("When the amount of gold is modified in nyStockpile then bcnStockpile will get the updated value")
    void test_6() throws InterruptedException {
        nyStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE);
        bcnStockpile.set(SILVER_STOCKPILE_KEY, SILVER_STOCKPILE_VALUE);

        // see explanation in the previous test
        Thread.sleep(1000);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, bcnStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, nyStockpile.get(SILVER_STOCKPILE_KEY));

    }

    @Test
    @Order(7)
    @DisplayName("When asEast is up and hkStockpile is created Then asEast joins to the grid")
    void test_7() throws ExecutionException, InterruptedException, TimeoutException {
        this.asEast = StorageGrid.builder()
                .withContext("AS east")
                .build();

        var keyCodec = Codec.<String, String>create(str -> str, str -> str);
        var valueCodec = Codec.<Integer, String>create(i -> i.toString(), str -> Integer.parseInt(str));
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        hkStockpile = asEast.<String, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();
        var countDownLatch = new CountDownLatch(2);

        euWest.joinedRemoteEndpoints().subscribe(euWestIsReady::complete);
        usEast.joinedRemoteEndpoints().subscribe(usEastIsReady::complete);
        asEast.joinedRemoteEndpoints().subscribe((uuid) -> {
            logger.info("AS EAST have a joined remote endpoint id {}", uuid);
            countDownLatch.countDown();
        });
        euWestIsReady.thenAccept((remoteEndpointId) -> {
            logger.info("EU West ({}) received a joined endpoint event, remote endpointId: {}", euWest.getLocalEndpointId(), remoteEndpointId);
        });
        usEastIsReady.thenAccept((remoteEndpointId) -> {
            logger.info("US East ({}) received a joined endpoint event, remote endpointId: {}", euWest.getLocalEndpointId(), remoteEndpointId);
        });

        this.router.add(asEast.getLocalEndpointId(), asEast.transport());

        if (!countDownLatch.await(10000, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("Have not joined to the grid");
        }
        logger.info("AS EAST remote endpoints are: {}", asEast.getRemoteEndpointIds());
        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);
    }
//
    @Test
    @Order(8)
    @DisplayName("When hkStockpile is updated to store bronze Then it is accessible at other stockpiles")
    void test_8() throws InterruptedException {
        hkStockpile.set(BRONZE_STOCKPILE_KEY, BRONZE_STOCKPILE_VALUE);

        Thread.sleep(1000);

        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, bcnStockpile.get(BRONZE_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, nyStockpile.get(BRONZE_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, hkStockpile.get(BRONZE_STOCKPILE_KEY));

    }
//
//
    @Test
    @Order(9)
    @DisplayName("When gold is removed from bcnStockpile Then it is removed from all stockpiles")
    void test_9() throws InterruptedException {
        bcnStockpile.delete(GOLD_STOCKPILE_KEY);

        Thread.sleep(1000);

        Assertions.assertNull(bcnStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertNull(nyStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertNull(hkStockpile.get(GOLD_STOCKPILE_KEY));

        Assertions.assertFalse(bcnStockpile.isEmpty());
        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertFalse(hkStockpile.isEmpty());

        Assertions.assertFalse(bcnStockpile.localIsEmpty());
        Assertions.assertFalse(nyStockpile.localIsEmpty());
        Assertions.assertFalse(hkStockpile.localIsEmpty());

        Assertions.assertEquals(2, bcnStockpile.localSize());
        Assertions.assertEquals(2, nyStockpile.localSize());
        Assertions.assertEquals(2, hkStockpile.localSize());
    }

    @Test
    @Order(10)
    @DisplayName("When silver is removed from bcnStockpile Then it is removed from all stockpiles")
    void test_10() throws InterruptedException {
        bcnStockpile.delete(SILVER_STOCKPILE_KEY);

        Thread.sleep(1000);

        Assertions.assertNull(bcnStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertNull(nyStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertNull(hkStockpile.get(SILVER_STOCKPILE_KEY));

        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertFalse(hkStockpile.isEmpty());

        Assertions.assertFalse(nyStockpile.localIsEmpty());
        Assertions.assertFalse(hkStockpile.localIsEmpty());

        Assertions.assertEquals(1, bcnStockpile.localSize());
        Assertions.assertEquals(1, nyStockpile.localSize());
        Assertions.assertEquals(1, hkStockpile.localSize());
    }
//
    @Test
    @Order(11)
    @DisplayName("When asEast is detached from the grid Then bronze stored in hkStockpile is accessible from the remaining storages")
    void test_11() throws ExecutionException, InterruptedException, TimeoutException {
        var stopped_1 = new CompletableFuture<UUID>();
        var stopped_2 = new CompletableFuture<UUID>();
        var leaderElected = new CompletableFuture<UUID>();
        var detachedEndpoints = new CountDownLatch(2);
        euWest.detachedRemoteEndpoints().subscribe(stopped_1::complete);
        usEast.detachedRemoteEndpoints().subscribe(stopped_2::complete);
        asEast.detachedRemoteEndpoints().subscribe((uuid) -> detachedEndpoints.countDown());

        if (UuidTools.equals(asEast.getLocalEndpointId(), asEast.getLeaderId())) {
            euWest.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(leaderElected::complete);
            usEast.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(leaderElected::complete);
        } else {
            leaderElected.complete(usEast.getLeaderId());
        }

        this.router.disable(asEast.getLocalEndpointId());

        CompletableFuture.allOf(stopped_1, stopped_2, leaderElected).get(20000, TimeUnit.MILLISECONDS);
        if (!detachedEndpoints.await(20000, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("usEast has not detached endpoints");
        }

        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, bcnStockpile.get(BRONZE_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, nyStockpile.get(BRONZE_STOCKPILE_KEY));
    }
//
    @Test
    @Order(12)
    @DisplayName("While asEast is away the storages are altered")
    void test_12() throws ExecutionException, InterruptedException, TimeoutException {
        var countdown = new CountDownLatch(2);
        nyStockpile.events().createdEntry().subscribe((e) -> countdown.countDown());
        bcnStockpile.events().createdEntry().subscribe((e) -> countdown.countDown());

        nyStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE);
        bcnStockpile.set(SILVER_STOCKPILE_KEY, SILVER_STOCKPILE_VALUE);

        if (!countdown.await(20000, TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Timeout");
        }
    }

    @Test
    @Order(13)
    @DisplayName("When asEast returns after log entries expired Then it requires a storagesync and stores only the data usEast and euWest stores")
    void test_13() throws InterruptedException, ExecutionException, TimeoutException {
        var countdown = new CountDownLatch(3);
        var started = new CompletableFuture<UUID>();
        asEast.joinedRemoteEndpoints().subscribe(started::complete);
        hkStockpile.events().createdEntry().subscribe(e -> countdown.countDown());

        Thread.sleep((long) (EXPIRATION_TIME_IN_MS * 1.5));
        this.router.enable(asEast.getLocalEndpointId());

        started.get(20000, TimeUnit.MILLISECONDS);

        if (!countdown.await(20000, TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Did not reached countdown");
        }

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, hkStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, hkStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, hkStockpile.get(BRONZE_STOCKPILE_KEY));
    }

}