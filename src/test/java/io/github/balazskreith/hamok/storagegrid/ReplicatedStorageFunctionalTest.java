package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.utils.CollectionUtils;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class ReplicatedStorageFunctionalTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageFunctionalTest.class);

    private static ReplicatedStoragesEnv environment;

    @BeforeAll
    static void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new ReplicatedStoragesEnv().create();
        environment.await();
        environment.awaitLeader(10000);
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
    void shouldSetAndGet() throws InterruptedException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        environment.awaitUntilCommitsSynced();
        var value = usStorage.get("one");

        Assertions.assertEquals(1, value);
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(1, usStorage.localSize());
    }

    @Test
    void shouldSetAndGetAll() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.setAll(Map.of("one", 1));
        environment.awaitUntilCommitsSynced();
        var entries = usStorage.getAll(Set.of("one"));

        Assertions.assertEquals(1, entries.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(1, euStorage.localSize());
    }

    @Test
    void shouldSetAndDelete_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        usStorage.delete("one");
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(0, euStorage.localSize());
        Assertions.assertEquals(0, usStorage.localSize());
    }

    @Test
    void shouldSetAndDelete_2() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.delete("one");
        environment.awaitUntilCommitsSynced();

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
        environment.awaitUntilCommitsSynced();

        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(1, usStorage.localSize());
    }

    @Test
    void shouldSetAndDeleteAndSet_2() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.delete("one");
        usStorage.set("one", 1);
        environment.awaitUntilCommitsSynced();

        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(1, usStorage.localSize());
    }

    @Test
    void shouldSetAndDeleteAll_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.setAll(Map.of("one", 1));
        usStorage.deleteAll(Set.of("one"));
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(0, euStorage.localSize());
        Assertions.assertEquals(0, euStorage.localSize());
    }

    @Test
    void shouldInsert_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        var euAlreadyInserted = euStorage.insert("one", 1);
        var usAlreadyInserted = euStorage.insert("one", 1);
        environment.awaitUntilCommitsSynced();

        Assertions.assertNull(euAlreadyInserted);
        Assertions.assertNotNull(usAlreadyInserted);
        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(1, usStorage.localSize());
    }

    @Test
    void shouldInsertAll_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        var euAlreadyInserted = euStorage.insertAll(Map.of("one", 1));
        var usAlreadyInserted = euStorage.insertAll(Map.of("one", 1));
        environment.awaitUntilCommitsSynced();

        Assertions.assertNull(euAlreadyInserted.get("one"));
        Assertions.assertNotNull(usAlreadyInserted.get("one"));
        Assertions.assertNotNull(euStorage.get("one"));
        Assertions.assertNotNull(usStorage.get("one"));
        Assertions.assertEquals(1, euStorage.localSize());
        Assertions.assertEquals(1, usStorage.localSize());
    }

    @Test
    void shouldNotBeEmpty() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        environment.awaitUntilCommitsSynced();

        Assertions.assertFalse(euStorage.isEmpty());
        Assertions.assertFalse(usStorage.isEmpty());
    }

    @Test
    void shouldBeCleared() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.clear();
        environment.awaitUntilCommitsSynced();

        Assertions.assertTrue(euStorage.isEmpty());
        Assertions.assertTrue(usStorage.isEmpty());
    }

    @Test
    void shouldGetAllKeys() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        environment.awaitUntilCommitsSynced();
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
        environment.awaitUntilCommitsSynced();

        var euIt = euStorage.iterator();
        var usIt = usStorage.iterator();

        Assertions.assertTrue(euIt.hasNext());
        Assertions.assertTrue(usIt.hasNext());
    }

}