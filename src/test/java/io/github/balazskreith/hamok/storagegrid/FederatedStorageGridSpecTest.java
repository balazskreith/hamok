package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("Federation Storage Specifications Test")
class FederatedStorageGridSpecTest {

    private static final Logger logger = LoggerFactory.getLogger(FederatedStorageStressTest.class);

    private static final int BCN_GOLD_STOCKPILE = 2;
    private static final int NY_GOLD_STOCKPILE = 3;
    private static final String GOLD_STOCKPILE = "gold";
    private static final String STORAGE_ID = "Stockpiles";

    private StorageGrid euWest;
    private StorageGrid usEast;
    private FederatedStorage<String, Integer> bcnStockpile;
    private FederatedStorage<String, Integer> nyStockpile;
    private StorageGridRouter router = new StorageGridRouter(Codec.create(JsonUtils::objectToBytes, bytes -> JsonUtils.bytesToObject(bytes, Message.class)));

    @Test
    @Order(1)
    @DisplayName("Setup")
    void test_1() throws InterruptedException {
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

        var countDownLatch = new CountDownLatch(2);
        this.euWest.joinedRemoteEndpoints().subscribe(uuid -> countDownLatch.countDown());
        this.usEast.joinedRemoteEndpoints().subscribe(uuid -> countDownLatch.countDown());

        this.router.add(this.euWest.getLocalEndpointId(), this.euWest.transport());
        this.router.add(this.usEast.getLocalEndpointId(), this.usEast.transport());

        if (!countDownLatch.await(10000, TimeUnit.MILLISECONDS)) {
            throw new IllegalStateException("Not connected");
        }
    }

    @Test
    @Order(2)
    @DisplayName("When Created it is empty")
    void test_2() {
        Assertions.assertTrue(bcnStockpile.isEmpty());
        Assertions.assertTrue(nyStockpile.isEmpty());
        Assertions.assertEquals(0, bcnStockpile.size());
        Assertions.assertEquals(0, nyStockpile.size());
    }
}