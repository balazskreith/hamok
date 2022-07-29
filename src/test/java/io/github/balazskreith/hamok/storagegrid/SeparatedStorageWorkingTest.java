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

@DisplayName("Separated Storage Working Test Scenario. While separated storages are distributed through the grid, endpoint can be joined and detached, but the storage should work as expected.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SeparatedStorageWorkingTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageWorkingTest.class);

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
    private SeparatedStorage<String, Integer> bcnStockpile;
    private SeparatedStorage<String, Integer> nyStockpile;
    private SeparatedStorage<String, Integer> hkStockpile;
    private StorageGridRouter router = new StorageGridRouter(Codec.create(JsonUtils::objectToBytes, bytes -> JsonUtils.bytesToObject(bytes, Message.class)));

    @Test
    @Order(1)
    @DisplayName("When euWest, and usEast are up and bcnStockpile, nyStockpile are created Then they are empty")
    void test_1() {
        this.euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withRaftMaxLogRetentionTimeInMs(30000)
                .build();
        this.usEast = StorageGrid.builder()
                .withContext("US east")
                .withRaftMaxLogRetentionTimeInMs(30000)
                .build();

        var keyCodec = Codec.<String, String>create(str -> str, str -> str);
        var valueCodec = Codec.<Integer, String>create(i -> i.toString(), str -> Integer.parseInt(str));
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        bcnStockpile = euWest.<String, Integer>separatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        nyStockpile = usEast.<String, Integer>separatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
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
    @DisplayName("When euWest and usEast are connected Then they notify the storages about the joined other endpoints")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();

        euWest.joinedRemoteEndpoints().subscribe(euWestIsReady::complete);
        usEast.joinedRemoteEndpoints().subscribe(usEastIsReady::complete);

        this.router.add(euWest.getLocalEndpointId(), euWest.transport());
        this.router.add(usEast.getLocalEndpointId(), usEast.transport());

        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);
    }

    @Test
    @Order(3)
    @DisplayName("When bcnStockpile have value for gold, and nyStockpile have value for silver Then nyStockpile can get gold, bcnStockpile can get value for silver")
    void test_3() {
        bcnStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE);
        nyStockpile.set(SILVER_STOCKPILE_KEY, SILVER_STOCKPILE_VALUE);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, nyStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, bcnStockpile.get(SILVER_STOCKPILE_KEY));

        Assertions.assertFalse(bcnStockpile.isEmpty());
        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertEquals(1, bcnStockpile.size());
        Assertions.assertEquals(1, nyStockpile.size());

        Assertions.assertEquals(1, bcnStockpile.localSize());
        Assertions.assertEquals(1, nyStockpile.localSize());
        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, bcnStockpile.localIterator().next().getValue());
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, nyStockpile.localIterator().next().getValue());

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, bcnStockpile.iterator().next().getValue());
        Assertions.assertEquals(SILVER_STOCKPILE_VALUE, nyStockpile.iterator().next().getValue());
    }

    @Test
    @Order(4)
    @DisplayName("When the amount of gold is modified in bcnStockpile then nyStockpile will get the updated value")
    void test_4() {
        bcnStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE + 1);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE + 1, nyStockpile.get(GOLD_STOCKPILE_KEY));

    }

    @Test
    @Order(5)
    @DisplayName("When the amount of gold is modified in nyStockpile then bcnStockpile will get the updated value")
    void test_5() {
        nyStockpile.set(GOLD_STOCKPILE_KEY, GOLD_STOCKPILE_VALUE);

        Assertions.assertEquals(GOLD_STOCKPILE_VALUE, bcnStockpile.get(GOLD_STOCKPILE_KEY));

    }

    @Test
    @Order(6)
    @DisplayName("When asEast is up and hkStockpile is created Then asEast joins to the grid")
    void test_6() throws ExecutionException, InterruptedException, TimeoutException {
        this.asEast = StorageGrid.builder()
                .withContext("AS east")
                .build();

        var keyCodec = Codec.<String, String>create(str -> str, str -> str);
        var valueCodec = Codec.<Integer, String>create(i -> i.toString(), str -> Integer.parseInt(str));
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        hkStockpile = asEast.<String, Integer>separatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
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

        this.router.add(asEast.getLocalEndpointId(), asEast.transport());

        if (!countDownLatch.await(10000, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("Have not joined to the grid");
        }
        logger.info("AS EAST remote endpoints are: {}", asEast.getRemoteEndpointIds());
        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);
    }

    @Test
    @Order(7)
    @DisplayName("When hkStockpile is updated to store bronze Then it is accessible at other stockpiles")
    void test_7() {
        hkStockpile.set(BRONZE_STOCKPILE_KEY, BRONZE_STOCKPILE_VALUE);

        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, bcnStockpile.get(BRONZE_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, nyStockpile.get(BRONZE_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, hkStockpile.get(BRONZE_STOCKPILE_KEY));

    }


    @Test
    @Order(8)
    @DisplayName("When gold is removed from bcnStockpile Then it is removed from all stockpiles")
    void test_8() {
        bcnStockpile.delete(GOLD_STOCKPILE_KEY);

        Assertions.assertNull(bcnStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertNull(nyStockpile.get(GOLD_STOCKPILE_KEY));
        Assertions.assertNull(hkStockpile.get(GOLD_STOCKPILE_KEY));

        Assertions.assertTrue(bcnStockpile.isEmpty());
        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertFalse(hkStockpile.isEmpty());

        Assertions.assertTrue(bcnStockpile.localIsEmpty());
        Assertions.assertFalse(nyStockpile.localIsEmpty());
        Assertions.assertFalse(hkStockpile.localIsEmpty());

        Assertions.assertEquals(0, bcnStockpile.localSize());
        Assertions.assertEquals(1, nyStockpile.localSize());
        Assertions.assertEquals(1, hkStockpile.localSize());
    }

    @Test
    @Order(9)
    @DisplayName("When silver is removed from bcnStockpile Then it is removed from all stockpiles")
    void test_9() {
        bcnStockpile.delete(SILVER_STOCKPILE_KEY);

        Assertions.assertNull(bcnStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertNull(nyStockpile.get(SILVER_STOCKPILE_KEY));
        Assertions.assertNull(hkStockpile.get(SILVER_STOCKPILE_KEY));

        Assertions.assertTrue(nyStockpile.isEmpty());
        Assertions.assertFalse(hkStockpile.isEmpty());

        Assertions.assertTrue(nyStockpile.localIsEmpty());
        Assertions.assertFalse(hkStockpile.localIsEmpty());

        Assertions.assertEquals(0, bcnStockpile.localSize());
        Assertions.assertEquals(0, nyStockpile.localSize());
        Assertions.assertEquals(1, hkStockpile.localSize());
    }

    @Test
    @Order(10)
    @DisplayName("When asEast is detached from the grid Then euWest and asEast took over the backup and the bronze stored in hkStockpile is accessable from the remaining storages")
    void test_10() throws ExecutionException, InterruptedException, TimeoutException {
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

        // add a bit time to change state in raccoon and fetch backups
        Thread.sleep(2000);

        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, bcnStockpile.get(BRONZE_STOCKPILE_KEY));
        Assertions.assertEquals(BRONZE_STOCKPILE_VALUE, nyStockpile.get(BRONZE_STOCKPILE_KEY));
    }

    @Test
    @Order(11)
    @DisplayName("When asEast returns to the grid, it is reset and storage is empty")
    void test_11() throws ExecutionException, InterruptedException, TimeoutException {
        var started = new CompletableFuture<UUID>();
        asEast.joinedRemoteEndpoints().subscribe(started::complete);
        this.router.enable(asEast.getLocalEndpointId());

        started.get(20000, TimeUnit.MILLISECONDS);

        Thread.sleep(1000);

        Assertions.assertEquals(0, hkStockpile.localSize());
    }

//    @Test
//    @Order(9)
//    @DisplayName("When ")
//    void test_9() {
//        throw new UnsupportedOperationException("Not implemented yet");
//    }
}