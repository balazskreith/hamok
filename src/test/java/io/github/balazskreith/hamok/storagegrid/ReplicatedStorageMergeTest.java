package io.github.balazskreith.hamok.storagegrid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.memorystorages.MemoryStorage;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;

@DisplayName("Replicated Storage Insert operation scenario.")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageMergeTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageMergeTest.class);

    private static final int EXPIRATION_TIME_IN_MS = 0;
    private static final String STORAGE_ID = "Stockpiles";

    private StorageGrid euWest;
    private StorageGrid usEast;
    private ReplicatedStorage<Integer, Value> bcnStockpile;
    private ReplicatedStorage<Integer, Value> nyStockpile;
    private StorageGridRouter router = new StorageGridRouter();

    private static class Value {
        public Integer key;
        public Set<Integer> values;
        public Integer lastValue;
    }

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
        var mapper = new ObjectMapper();
        Function<Value, byte[]> valueEnc = o -> {
            try {
                return mapper.writeValueAsBytes(o);
            } catch (JsonProcessingException e) {
                return null;
            }
        };
        Function<byte[], Value> valueDec = b -> {
            try {
                return mapper.readValue(b, Value.class);
            } catch (IOException e) {
                return null;
            }
        };
        BinaryOperator<Value> mergeOp = (oldValue, newValue) -> {
            var result = new Value();
            result.key = oldValue.key;
            result.values = SetUtils.combineAll(oldValue.values, newValue.values);
            result.lastValue = newValue.lastValue;
            return result;
        };

        bcnStockpile = euWest.<Integer, Value>replicatedStorage()
                .setStorage(MemoryStorage.<Integer, Value>builder()
                        .setId(STORAGE_ID)
                        .setConcurrency(true)
                        .setMergeOp(mergeOp)
                        .build()
                )
                .setKeyCodec(intEnc, intDec)
                .setValueCodec(valueEnc, valueDec)
                .build();

        nyStockpile = usEast.<Integer, Value>replicatedStorage()
                .setStorage(MemoryStorage.<Integer, Value>builder()
                        .setId(STORAGE_ID)
                        .setConcurrency(true)
                        .setMergeOp(mergeOp)
                        .build()
                )
                .setKeyCodec(intEnc, intDec)
                .setValueCodec(valueEnc, valueDec)
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
    @DisplayName("Should set values and merge sets")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var key = 3;
        var lastValue = new AtomicInteger();
        BiConsumer<ReplicatedStorage<Integer, Value>, Integer> set = (storage, num) -> {
            var value = new Value();
            value.key = key;
            value.values = Set.of(num);
            value.lastValue = num;
            storage.set(key, value);
            lastValue.set(num);
        };
        Set<Integer> addedNums = new HashSet<>();
        Function<ReplicatedStorage<Integer, Value>, Runnable> createProcess = storage -> () -> {
            var random = new Random();
            for (int i = 0; i < 10; ++i) {
                var num = random.nextInt();
                set.accept(storage, num);
                addedNums.add(num);

            }
        };
        var t1 = new Thread(createProcess.apply(nyStockpile));
        var t2 = new Thread(createProcess.apply(bcnStockpile));

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        var nyValue = nyStockpile.get(key);
        var bcnValue = bcnStockpile.get(key);
        for (var num : addedNums) {
            Assertions.assertTrue(nyValue.values.contains(num));
            Assertions.assertTrue(bcnValue.values.contains(num));
            Assertions.assertEquals(lastValue.get(), nyValue.lastValue);
            Assertions.assertEquals(lastValue.get(), bcnValue.lastValue);
        }
    }

    @Test
    @Order(3)
    @DisplayName("Should set all values and merge sets")
    void test_3() throws ExecutionException, InterruptedException, TimeoutException {
        var key = 3;
        var lastValue = new AtomicInteger();
        BiConsumer<ReplicatedStorage<Integer, Value>, Integer> set = (storage, num) -> {
            var value = new Value();
            value.key = key;
            value.values = Set.of(num);
            value.lastValue = num;
            storage.setAll(Map.of(key, value));
            lastValue.set(num);
        };
        Set<Integer> addedNums = new HashSet<>();
        Function<ReplicatedStorage<Integer, Value>, Runnable> createProcess = storage -> () -> {
            var random = new Random();
            for (int i = 0; i < 10; ++i) {
                var num = random.nextInt();
                set.accept(storage, num);
                addedNums.add(num);
            }
        };
        var t1 = new Thread(createProcess.apply(nyStockpile));
        var t2 = new Thread(createProcess.apply(bcnStockpile));

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        var nyValue = nyStockpile.get(key);
        var bcnValue = bcnStockpile.get(key);
        for (var num : addedNums) {
            Assertions.assertTrue(nyValue.values.contains(num));
            Assertions.assertTrue(bcnValue.values.contains(num));
            Assertions.assertEquals(lastValue.get(), nyValue.lastValue);
            Assertions.assertEquals(lastValue.get(), bcnValue.lastValue);
        }
    }

}