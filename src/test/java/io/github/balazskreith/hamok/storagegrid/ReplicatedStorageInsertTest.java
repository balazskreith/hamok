package io.github.balazskreith.hamok.storagegrid;

import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Function;

@DisplayName("Replicated Storage Insert operation scenario.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageInsertTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageInsertTest.class);

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
    }

    @Test
    @Order(2)
    @DisplayName("Should not insert values twice")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var countdown = new CountDownLatch(2);
        var key = 3;
        var inserted = new LinkedList<Map<Integer, Integer>>();
        BiConsumer<ReplicatedStorage<Integer, Integer>, Integer> insert = (storage, value) -> {
            var candidate = Map.of(key, value);
            var alreadyInserted = storage.insertAll(candidate);
            if (alreadyInserted.size() < 1) {
                inserted.add(candidate);
            } else {
                inserted.add(alreadyInserted);
            }
            countdown.countDown();;
        };
        Schedulers.io().scheduleDirect(() -> insert.accept(nyStockpile, 1), (int) (Math.random() * 1000), TimeUnit.MILLISECONDS);
        Schedulers.io().scheduleDirect(() -> insert.accept(bcnStockpile, 2), (int) (Math.random() * 1000), TimeUnit.MILLISECONDS);

        if (!countdown.await(20000, TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Timeout");
        }
        Assertions.assertEquals(2, inserted.size());
        Assertions.assertEquals(inserted.get(0).get(key), inserted.get(1).get(key));
    }

    private static Integer getRandomNumber() {
        return (int) (Math.random() * (Integer.MAX_VALUE - 1));
    }


}