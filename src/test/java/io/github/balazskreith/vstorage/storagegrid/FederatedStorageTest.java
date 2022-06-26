package io.github.balazskreith.vstorage.storagegrid;

import io.github.balazskreith.vstorage.mappings.Codec;
import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FederatedStorageTest {

    private static final String STORAGE_ID = "Stockpiles";
    private static StorageGrid euWest;
    private static StorageGrid usEast;
    private static FederatedStorage<String, Integer> bcnStockPile;
    private static FederatedStorage<String, Integer> nyStockPile;
//    private FederatedStorage<String, Integer>

    @BeforeAll
    public static void setup() throws InterruptedException, ExecutionException, TimeoutException {
        euWest = StorageGrid.builder().build();
        usEast = StorageGrid.builder().build();

        var euWestIsReady = new CompletableFuture<UUID>();
        var usEastIsReady = new CompletableFuture<UUID>();
        euWest.joinedRemoteEndpoints().subscribe(euWestIsReady::complete);
        usEast.joinedRemoteEndpoints().subscribe(usEastIsReady::complete);

        euWest.transport().getSender().subscribe(usEast.transport().getReceiver());
        usEast.transport().getSender().subscribe(euWest.transport().getReceiver());

        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);

        var keyCodec = Codec.<String, String>create(str -> str, str -> str);
        var valueCodec = Codec.<Integer, String>create(i -> i.toString(), str -> Integer.parseInt(str));

        bcnStockPile = euWest.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMergeOperator(() -> (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2)
                .setKeyCodecSupplier(() -> keyCodec)
                .setValueCodecSupplier(() -> valueCodec)
                .build();

        nyStockPile = usEast.<String, Integer>federatedStorage()
                .setStorageId(STORAGE_ID)
                .setMergeOperator(() -> (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2)
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
    @DisplayName("When a value for the same key is added at two different federeated storage, Then getting that key is a merged result on any of the federated storage")
    void shouldAddItemsToStockPiles() {
        bcnStockPile.put("gold", 1);
        nyStockPile.put("gold", 2);

        Assertions.assertEquals(3, bcnStockPile.get("gold"));
        Assertions.assertEquals(3, nyStockPile.get("gold"));
    }

    @Test
    @Order(3)
    @DisplayName("When a value for the same key is added at two different federeated storage, Then getting that key is a merged result on any of the federated storage")
    void shouldBeNotEmpty() {
        Assertions.assertFalse(bcnStockPile.isEmpty());
        Assertions.assertFalse(nyStockPile.isEmpty());
        Assertions.assertEquals(1, bcnStockPile.size());
        Assertions.assertEquals(1, nyStockPile.size());
    }

    @Test
    @Order(4)
    @DisplayName("When a key is removed in one federated storage, the result of get request is the result of the other")
    void shouldRemoveItemsToStockPiles() {
        bcnStockPile.delete("gold");

        Assertions.assertEquals(2, bcnStockPile.get("gold"));
        Assertions.assertEquals(2, nyStockPile.get("gold"));
    }

    @Test
    @Order(5)
    @DisplayName("When a key is removed in one federated storage, the result of get request is the result of the other")
    void bcnStockpileShouldBeEmpty() {
        Assertions.assertTrue(bcnStockPile.isEmpty());
        Assertions.assertFalse(nyStockPile.isEmpty());
        Assertions.assertEquals(0, bcnStockPile.size());
        Assertions.assertEquals(1, nyStockPile.size());
    }

}