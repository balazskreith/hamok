package io.github.balazskreith.hamok.storagegrid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@DisplayName("Federation Storage Stress Test Scenario. While federated storages are distributed through the grid, endpoint can be joined and detached, but the storage remains consequent.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PropagatedCollectionsFunctionalTest {

    private static final Logger logger = LoggerFactory.getLogger(PropagatedCollectionsFunctionalTest.class);

    private static final int BCN_GOLD_STOCKPILE = 2;
    private static final int NY_GOLD_STOCKPILE = 3;
    private static final int HK_GOLD_STOCKPILE = 5;
    private static final String GOLD_STOCKPILE = "gold";
    private static final String STORAGE_ID = "Stockpiles";

    private StorageGrid euWest;
    private StorageGrid usEast;
    private StorageGrid asEast;
    private PropagatedCollections<String, Integer, Set<Integer>> bcnStockpile;
    private PropagatedCollections<String, Integer, Set<Integer>> nyStockpile;
    private PropagatedCollections<String, Integer, Set<Integer>> hkStockpile;
    private StorageGridRouter router = new StorageGridRouter();

    @Test
    @Order(1)
    @DisplayName("Setup")
    void test_1() {
        this.euWest = StorageGrid.builder()
                .withContext("Eu West")
                .withRaftMaxLogRetentionTimeInMs(30000)
                .build();
        this.usEast = StorageGrid.builder()
                .withContext("US east")
                .withRaftMaxLogRetentionTimeInMs(30000)
                .build();

        var mapper = new ObjectMapper();
        Function<Set<Integer>, byte[]> setEnc = i -> {
            try {
                return mapper.writeValueAsBytes(i);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        };
        Function<byte[], Set<Integer>> setDec = b -> {
            try {
                return mapper.readValue(b, Set.class);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        };
        Function<String, byte[]> strEnc = s -> s.getBytes();
        Function<byte[], String> strDec = b -> new String(b);

        bcnStockpile = euWest.<String, Integer>propagatedSets()
                .setGridEndpointId(STORAGE_ID)
                .setKeyCodec(strEnc, strDec)
                .setCollectionCodec(setEnc, setDec)
                .build();

        nyStockpile = usEast.<String, Integer>propagatedSets()
                .setGridEndpointId(STORAGE_ID)
                .setKeyCodec(strEnc, strDec)
                .setCollectionCodec(setEnc, setDec)
                .build();

        Assertions.assertTrue(bcnStockpile.isEmpty());
        Assertions.assertTrue(nyStockpile.isEmpty());
        Assertions.assertEquals(0, bcnStockpile.size());
        Assertions.assertEquals(0, nyStockpile.size());
    }

    @Test
    @Order(2)
    @DisplayName("Join to a grid")
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
    @DisplayName("Add to propagated set")
    void test_3() throws InterruptedException {
        bcnStockpile.add(GOLD_STOCKPILE, BCN_GOLD_STOCKPILE);
        nyStockpile.add(GOLD_STOCKPILE, NY_GOLD_STOCKPILE);

        Thread.sleep(1000);

        var stockpiles = bcnStockpile.get(GOLD_STOCKPILE);

        Assertions.assertTrue(stockpiles.contains(BCN_GOLD_STOCKPILE));
        Assertions.assertTrue(stockpiles.contains(NY_GOLD_STOCKPILE));
    }

    @Test
    @Order(4)
    @DisplayName("delete to propagated set")
    void test_4() throws InterruptedException {
        bcnStockpile.remove(GOLD_STOCKPILE, NY_GOLD_STOCKPILE);

        Thread.sleep(1000);

        var stockpiles = bcnStockpile.get(GOLD_STOCKPILE);

        Assertions.assertTrue(stockpiles.contains(BCN_GOLD_STOCKPILE));
        Assertions.assertFalse(stockpiles.contains(NY_GOLD_STOCKPILE));
    }

}