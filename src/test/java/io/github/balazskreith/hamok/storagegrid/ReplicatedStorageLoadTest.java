package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.function.Function;

@DisplayName("Replicated Storage Working Test Scenario. While replicated storages are distributed through the grid, endpoint can be joined and detached, but the storage should work as expected.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageLoadTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageLoadTest.class);

//    private static final int EXPIRATION_TIME_IN_MS = 5000;
    private static final int EXPIRATION_TIME_IN_MS = 15000;
    private static final String STORAGE_ID = "Stockpiles";


    private StorageGrid euWest;
    private StorageGrid usEast;
    private ReplicatedStorage<Integer, Integer> bcnStockpile;
    private ReplicatedStorage<Integer, Integer> nyStockpile;
    private StorageGridRouter router = new StorageGridRouter();

    @Test
    @Order(1)
    @DisplayName("Setup")
    void test_1() throws InterruptedException, ExecutionException, TimeoutException {
        this.euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
                .build();
        this.usEast = StorageGrid.builder()
                .withContext("US east")
                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
                .build();
        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        bcnStockpile = euWest.<Integer, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setKeyCodec(intEnc, intDec)
                .setValueCodec(intEnc, intDec)
                .build();

        nyStockpile = usEast.<Integer, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setKeyCodec(intEnc, intDec)
                .setValueCodec(intEnc, intDec)
                .build();

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
    @Order(2)
    @DisplayName("When euWest and usEast are connected Then they elect a leader and that leader id should be the same")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var entries = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100000; ++i) {
            int key = getRandomNumber();
            int value = getRandomNumber();
            entries.put(key, value);
        }

        nyStockpile.insertAll(entries);
    }

    private static Integer getRandomNumber() {
        return (int) (Math.random() * (Integer.MAX_VALUE - 1));
    }


}