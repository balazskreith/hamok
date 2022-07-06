package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.ModifiedStorageEntry;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.mappings.Codec;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;

@DisplayName("Federation Storage Stress Test Scenario. While federated storages are distributed through the grid, endpoint can be joined and detached, but the storage remains consequent.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FederatedStorageFunctionalTest {

    private static final Logger logger = LoggerFactory.getLogger(FederatedStorageFunctionalTest.class);

    private static final int BCN_GOLD_STOCKPILE = 2;
    private static final int NY_GOLD_STOCKPILE = 3;
    private static final int HK_GOLD_STOCKPILE = 5;
    private static final String GOLD_STOCKPILE = "gold";
    private static final String STORAGE_ID = "Stockpiles";

    private StorageGrid euWest;
    private StorageGrid usEast;
    private StorageGrid asEast;
    private FederatedStorage<String, Integer> bcnStockpile;
    private FederatedStorage<String, Integer> nyStockpile;
    private FederatedStorage<String, Integer> hkStockpile;
    private StorageGridRouter router = new StorageGridRouter();

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

        var keyCodec = Codec.<String, byte[]>create(str -> str.getBytes(StandardCharsets.UTF_8), bytes -> new String(bytes));
        var valueCodec = Codec.<Integer, byte[]>create(i -> ByteBuffer.allocate(4).putInt(i).array(), arr -> ByteBuffer.wrap(arr).getInt());
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        bcnStockpile = euWest.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
                .setMergeOperator(() -> mergeOp)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        nyStockpile = usEast.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
                .setMergeOperator(() -> mergeOp)
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
    @DisplayName("When bcnStockpile and nyStockpile have value for key gold, Then the value to retrieve the amount of gold on any federated storage is the merged value of all")
    void test_3() {
        bcnStockpile.set(GOLD_STOCKPILE, BCN_GOLD_STOCKPILE);
        nyStockpile.set(GOLD_STOCKPILE, NY_GOLD_STOCKPILE);

        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, bcnStockpile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, nyStockpile.get(GOLD_STOCKPILE));

        Assertions.assertFalse(bcnStockpile.isEmpty());
        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertEquals(1, bcnStockpile.size());
        Assertions.assertEquals(1, nyStockpile.size());

        Assertions.assertEquals(1, bcnStockpile.localSize());
        Assertions.assertEquals(1, nyStockpile.localSize());
        Assertions.assertEquals(BCN_GOLD_STOCKPILE, bcnStockpile.localIterator().next().getValue());
        Assertions.assertEquals(NY_GOLD_STOCKPILE, nyStockpile.localIterator().next().getValue());

        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, bcnStockpile.iterator().next().getValue());
        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, nyStockpile.iterator().next().getValue());
    }

    @Test
    @Order(4)
    @DisplayName("When asEast is up and hkStockpile is created Then asEast joins to the grid")
    void test_4() throws ExecutionException, InterruptedException, TimeoutException {
        this.asEast = StorageGrid.builder()
                .withContext("AS east")
                .build();

        var keyCodec = Codec.<String, byte[]>create(str -> str.getBytes(StandardCharsets.UTF_8), bytes -> new String(bytes));
        var valueCodec = Codec.<Integer, byte[]>create(i -> ByteBuffer.allocate(4).putInt(i).array(), arr -> ByteBuffer.wrap(arr).getInt());
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        hkStockpile = asEast.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
                .setMergeOperator(() -> mergeOp)
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
    @Order(5)
    @DisplayName("When hkStockpile is updated to store gold Then the total gold is the amount of gold all federated storage stores")
    void test_5() {
        hkStockpile.set(GOLD_STOCKPILE, HK_GOLD_STOCKPILE);

        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, bcnStockpile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, nyStockpile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, hkStockpile.get(GOLD_STOCKPILE));

        Assertions.assertFalse(hkStockpile.isEmpty());
        Assertions.assertEquals(1, hkStockpile.size());

        Assertions.assertEquals(1, hkStockpile.localSize());
        Assertions.assertEquals(HK_GOLD_STOCKPILE, hkStockpile.localIterator().next().getValue());

        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, hkStockpile.iterator().next().getValue());
    }


    @Test
    @Order(6)
    @DisplayName("When a key is removed in one bcnStockpile Then it is remove from all stockpiles")
    void test_6() throws InterruptedException {
        bcnStockpile.delete(GOLD_STOCKPILE);

        Assertions.assertEquals(NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, bcnStockpile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, nyStockpile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, hkStockpile.get(GOLD_STOCKPILE));

        Assertions.assertFalse(bcnStockpile.isEmpty());
        Assertions.assertFalse(nyStockpile.isEmpty());
        Assertions.assertFalse(hkStockpile.isEmpty());

        Assertions.assertTrue(bcnStockpile.localIsEmpty());
        Assertions.assertFalse(nyStockpile.localIsEmpty());
        Assertions.assertFalse(hkStockpile.localIsEmpty());
        Assertions.assertEquals(1, bcnStockpile.size());
        Assertions.assertEquals(1, nyStockpile.size());
        Assertions.assertEquals(1, hkStockpile.size());

        Assertions.assertEquals(0, bcnStockpile.localSize());
        Assertions.assertEquals(1, nyStockpile.localSize());
        Assertions.assertEquals(1, hkStockpile.localSize());

        Assertions.assertFalse( bcnStockpile.localIterator().hasNext());
        Assertions.assertEquals(NY_GOLD_STOCKPILE, nyStockpile.localIterator().next().getValue());
        Assertions.assertFalse(bcnStockpile.localIterator().hasNext());
        Assertions.assertEquals(HK_GOLD_STOCKPILE, hkStockpile.localIterator().next().getValue());
    }

    @Test
    @Order(7)
    @DisplayName("When usEast is detached from the grid Then euWest and asEast took over the backup and the total amount of gold does not change")
    void test_7() throws ExecutionException, InterruptedException, TimeoutException {
        var stopped_1 = new CompletableFuture<UUID>();
        var stopped_2 = new CompletableFuture<UUID>();
        var leaderElected = new CompletableFuture<UUID>();
        var detachedEndpoints = new CountDownLatch(2);
        euWest.detachedRemoteEndpoints().subscribe(stopped_1::complete);
        asEast.detachedRemoteEndpoints().subscribe(stopped_2::complete);
        usEast.detachedRemoteEndpoints().subscribe((uuid) -> detachedEndpoints.countDown());

        if (UuidTools.equals(usEast.getLocalEndpointId(), usEast.getLeaderId())) {
            euWest.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(leaderElected::complete);
            asEast.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(leaderElected::complete);
        } else {
            leaderElected.complete(usEast.getLeaderId());
        }

        this.router.disable(usEast.getLocalEndpointId());

        CompletableFuture.allOf(stopped_1, stopped_2, leaderElected).get(20000, TimeUnit.MILLISECONDS);
        if (!detachedEndpoints.await(20000, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("usEast has not detached endpoints");
        }

        // add a bit time to change state in raccoon and fetch backups
        Thread.sleep(2000);

        Assertions.assertEquals(NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, bcnStockpile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(NY_GOLD_STOCKPILE + HK_GOLD_STOCKPILE, hkStockpile.get(GOLD_STOCKPILE));
    }

    @Test
    @Order(8)
    @DisplayName("When usEast returns to the grid, it is reset and storage is empty")
    void test_8() throws ExecutionException, InterruptedException, TimeoutException {
        var started = new CompletableFuture<UUID>();
        var evicted = new CompletableFuture<List<ModifiedStorageEntry<String, Integer>>>();
        usEast.joinedRemoteEndpoints().subscribe(started::complete);

        this.nyStockpile.events()
                .collectOn(Schedulers.io(), 1000, 10000)
                .evictedEntries()
                .subscribe(evicted::complete);

        this.router.enable(usEast.getLocalEndpointId());

        started.get(20000, TimeUnit.MILLISECONDS);

        evicted.get(10000, TimeUnit.MILLISECONDS);

        Assertions.assertEquals(0, nyStockpile.localSize());
    }

//    @Test
//    @Order(9)
//    @DisplayName("When ")
//    void test_9() {
//        throw new UnsupportedOperationException("Not implemented yet");
//    }
}