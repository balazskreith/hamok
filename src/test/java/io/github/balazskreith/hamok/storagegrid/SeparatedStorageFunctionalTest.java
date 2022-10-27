package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.utils.CollectionUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class SeparatedStorageFunctionalTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageFunctionalTest.class);

    private static SeparatedStoragesEnv environment;

    @BeforeAll
    static void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new SeparatedStoragesEnv()
                .setRequestMessageLimits(1, 1, collidingItem -> {
                    logger.warn("Detected colliding key {} value1: {}, value2: {}", collidingItem.key(), collidingItem.value1(), collidingItem.value2());
                })
                .create();
        environment.await();
    }

    @AfterAll
    static void teardown() {
        environment.destroy();
    }

    @AfterEach
    void reset() {
        environment.clear();
    }

    @Test
    void shouldSetAndGet() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        var value = usStorage.get("one");

        Assertions.assertEquals(1, value);
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldSetAndGetAll() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.setAll(Map.of("one", 1));
        var entries = usStorage.getAll(Set.of("one"));

        Assertions.assertEquals(1, entries.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldSetAndDelete_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        usStorage.delete("one");
        Assertions.assertEquals(0, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldSetAndDelete_2() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.delete("one");
        Assertions.assertNull(euStorage.get("one"));
        Assertions.assertNull(usStorage.get("one"));
    }

    @Test
    void shouldSetAndDeleteAndSet_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.delete("one");
        euStorage.set("one", 1);
        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldSetAndDeleteAndSet_2() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.delete("one");
        usStorage.set("one", 1);
        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(0, euStorage.localSize());
        Assertions.assertEquals(1, usStorage.localSize());
    }

    @Test
    void shouldSetAndDeleteAll_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.setAll(Map.of("one", 1));
        usStorage.deleteAll(Set.of("one"));
        Assertions.assertEquals(0, euStorage.localSize());
        Assertions.assertEquals(0, euStorage.localSize());
    }

    @Test
    void shouldInsert_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        var euAlreadyInserted = euStorage.insert("one", 1);
        var usAlreadyInserted = euStorage.insert("one", 1);

        Assertions.assertNull(euAlreadyInserted);
        Assertions.assertNotNull(usAlreadyInserted);
        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldInsertAll_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        var euAlreadyInserted = euStorage.insertAll(Map.of("one", 1));
        var usAlreadyInserted = euStorage.insertAll(Map.of("one", 1));

        Assertions.assertNull(euAlreadyInserted.get("one"));
        Assertions.assertNotNull(usAlreadyInserted.get("one"));
        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldGetAll_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        usStorage.set("two", 2);
        var entriesFromEu = euStorage.getAll(Set.of("one", "two"));
        var entriesFromUs = euStorage.getAll(Set.of("one", "two"));

        Assertions.assertEquals(1, entriesFromEu.get("one"));
        Assertions.assertEquals(2, entriesFromEu.get("two"));
        Assertions.assertEquals(1, entriesFromUs.get("one"));
        Assertions.assertEquals(2, entriesFromUs.get("two"));
    }

    @Test
    void shouldGetAll_2() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        usStorage.set("two", 2);
        usStorage.set("three", 3);
        var entriesFromEu = euStorage.getAll(Set.of("one", "two", "three"));
    }

    @Test
    void shouldNotBeEmpty() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);

        Assertions.assertFalse(euStorage.isEmpty());
        Assertions.assertFalse(usStorage.isEmpty());
    }

    @Test
    void shouldBeCleared() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.clear();

        Assertions.assertTrue(euStorage.isEmpty());
        Assertions.assertTrue(usStorage.isEmpty());
    }

    @Test
    void shouldGetAllKeys() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        var euKeys = euStorage.keys();
        var usKeys = usStorage.keys();

        Assertions.assertTrue(0 < euKeys.size());
        Assertions.assertTrue(CollectionUtils.equalCollections(euKeys, usKeys));
    }

    @Test
    void shouldIterate() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        var euIt = euStorage.iterator();
        var usIt = usStorage.iterator();

        Assertions.assertTrue(euIt.hasNext());
        Assertions.assertTrue(usIt.hasNext());
    }

    @Test
    void shouldEvict() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        Assertions.assertThrows(Exception.class, () -> {
            euStorage.set("one", 1);
            usStorage.evict("one");
        });
    }

    @Test
    void shouldEvictAll() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        Assertions.assertThrows(Exception.class, () -> {
            euStorage.set("one", 1);
            usStorage.evictAll(Set.of("one"));
        });
    }

    @Test
    void shouldRestore() {
        var euStorage = environment.getEuStorage();

        Assertions.assertThrows(Exception.class, () -> {
            euStorage.restore("one", 1);
        });
    }

    @Test
    void shouldRestoreAll() {
        var euStorage = environment.getEuStorage();

        Assertions.assertThrows(Exception.class, () -> {
            euStorage.restoreAll(Map.of("one", 1));
        });
    }


}