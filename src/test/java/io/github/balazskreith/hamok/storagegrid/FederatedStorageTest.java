package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.mappings.Codec;
import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FederatedStorageTest {
    private static final int BCN_GOLD_STOCKPILE = 6;
    private static final int NY_GOLD_STOCKPILE = 5;
    private static final String GOLD_STOCKPILE = "gold";
    private static final String STORAGE_ID = "Stockpiles";

    private static StorageGrid euWest;
    private static StorageGrid usEast;
    private static FederatedStorage<String, Integer> bcnStockPile;
    private static FederatedStorage<String, Integer> nyStockPile;
    private static StorageGridTransport euWestTransport;
    private static StorageGridTransport usEastTransport;
//    private FederatedStorage<String, Integer>

    @BeforeAll
    public static void setup() throws InterruptedException, ExecutionException, TimeoutException {
        euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withAutoJoin(true)
                .build();
        usEast = StorageGrid.builder()
                .withContext("US east")
                .withAutoJoin(true)
                .build();

        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();
        euWest.joinedRemoteEndpoints().subscribe(euWestIsReady::complete);
        usEast.joinedRemoteEndpoints().subscribe(usEastIsReady::complete);

        euWestTransport = euWest.transport();
        usEastTransport = usEast.transport();
        euWestTransport.getSender().subscribe(usEastTransport.getReceiver());
        usEastTransport.getSender().subscribe(euWestTransport.getReceiver());

        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);

        var keyCodec = Codec.<String, String>create(str -> str, str -> str);
        var valueCodec = Codec.<Integer, String>create(i -> i.toString(), str -> Integer.parseInt(str));
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        bcnStockPile = euWest.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMergeOperator(() -> mergeOp)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        nyStockPile = usEast.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMergeOperator(() -> mergeOp)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();
    }

    @Test
    @Order(1)
    @DisplayName("When created Then it is empty")
    void shouldBeEmpty() {
        Assertions.assertTrue(bcnStockPile.isEmpty());
        Assertions.assertTrue(nyStockPile.isEmpty());
        Assertions.assertEquals(0, bcnStockPile.size());
        Assertions.assertEquals(0, nyStockPile.size());
    }

    @Test
    @Order(2)
    @DisplayName("When a value for the same key is added to two different federated storage, Then getting that key is a merged result on any of the federated storage")
    void shouldAddItemsToStockPiles() {
        bcnStockPile.set(GOLD_STOCKPILE, BCN_GOLD_STOCKPILE);
        nyStockPile.set(GOLD_STOCKPILE, NY_GOLD_STOCKPILE);

        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, bcnStockPile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, nyStockPile.get(GOLD_STOCKPILE));

        Assertions.assertFalse(bcnStockPile.isEmpty());
        Assertions.assertFalse(nyStockPile.isEmpty());
        Assertions.assertEquals(1, bcnStockPile.size());
        Assertions.assertEquals(1, nyStockPile.size());
        Assertions.assertEquals(1, bcnStockPile.localSize());
        Assertions.assertEquals(1, nyStockPile.localSize());
        Assertions.assertEquals(BCN_GOLD_STOCKPILE, bcnStockPile.localIterator().next().getValue());
        Assertions.assertEquals(NY_GOLD_STOCKPILE, nyStockPile.localIterator().next().getValue());
        Assertions.assertEquals(BCN_GOLD_STOCKPILE + NY_GOLD_STOCKPILE, nyStockPile.iterator().next().getValue());
    }


    @Test
    @Order(3)
    @DisplayName("When a key is removed in one federated storage, the result of get request is the result of the other")
    void shouldRemoveItemsToStockPiles() {
        bcnStockPile.delete(GOLD_STOCKPILE);

        Assertions.assertEquals(NY_GOLD_STOCKPILE, bcnStockPile.get(GOLD_STOCKPILE));
        Assertions.assertEquals(NY_GOLD_STOCKPILE, nyStockPile.get(GOLD_STOCKPILE));

        Assertions.assertFalse(bcnStockPile.isEmpty());
        Assertions.assertFalse(nyStockPile.isEmpty());
        Assertions.assertTrue(bcnStockPile.localIsEmpty());
        Assertions.assertFalse(nyStockPile.localIsEmpty());
        Assertions.assertEquals(1, bcnStockPile.size());
        Assertions.assertEquals(1, nyStockPile.size());
        Assertions.assertEquals(0, bcnStockPile.localSize());
        Assertions.assertEquals(1, nyStockPile.localSize());

        Assertions.assertFalse( bcnStockPile.localIterator().hasNext());
        Assertions.assertEquals(NY_GOLD_STOCKPILE, nyStockPile.localIterator().next().getValue());
        Assertions.assertEquals(NY_GOLD_STOCKPILE, nyStockPile.iterator().next().getValue());
    }

    @Test
    @Order(4)
    @DisplayName("When a us west stockpiles disconnected from the grid, the backup goes to eu west")
    void shouldHaveBackup() throws ExecutionException, InterruptedException, TimeoutException {
        var stopped = new CompletableFuture<UUID>();
        usEast.detachedRemoteEndpoints().subscribe(stopped::complete);
        usEast.detach();

        stopped.get(20000, TimeUnit.MILLISECONDS);

        Thread.sleep(5000);

        Assertions.assertEquals(0, bcnStockPile.localSize());
    }

}