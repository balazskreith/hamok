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
import java.util.function.Function;

@DisplayName("Replicated Storage Insert operation scenario.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageSyncTest.class);

//    private static final int EXPIRATION_TIME_IN_MS = 5000;
    private static final int EXPIRATION_TIME_IN_MS = 5000;
    private static final String STORAGE_ID = "Stockpiles";

    private StorageGrid euWest;
    private StorageGrid usEast;
    private StorageGrid asEast;
    private ReplicatedStorage<Integer, Integer> bcnStockpile;
    private ReplicatedStorage<Integer, Integer> nyStockpile;
    private ReplicatedStorage<Integer, Integer> hkStockpile;
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
    @DisplayName("Fill the replicated storage and keep it in this stage more than the rat expiration log time")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 0; i < 10; ++i) {
            var entries = new HashMap<Integer, Integer>();
            for (int j = 0; j < 1000; ++j) {
                entries.put(i * 1000 + j, getRandomNumber());
            }
            this.nyStockpile.insertAll(entries);
            Thread.sleep(1000);
        }
    }

    @Test
    @Order(3)
    @DisplayName("Add new participant")
    void test_3() throws ExecutionException, InterruptedException, TimeoutException {
        this.asEast = StorageGrid.builder()
                .withContext("AS east")
                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
                .build();

        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
        hkStockpile = asEast.<Integer, Integer>replicatedStorage()
                .setStorageId(STORAGE_ID)
                .setKeyCodec(intEnc, intDec)
                .setValueCodec(intEnc, intDec)
                .build();

        var asEastIsReady = new CompletableFuture<>();
        asEast.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(asEastIsReady::complete);

        this.router.add(asEast.getLocalEndpointId(), asEast.transport());
        asEastIsReady.get();
    }

    @Test
    @Order(3)
    @DisplayName("Add new participant")
    void test_4() throws ExecutionException, InterruptedException, TimeoutException {
        Thread.sleep(30000);
    }

    private static Integer getRandomNumber() {
        return (int) (Math.random() * (Integer.MAX_VALUE - 1));
    }


}