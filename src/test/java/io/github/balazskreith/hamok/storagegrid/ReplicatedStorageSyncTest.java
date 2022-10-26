package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageSyncTest.class);

    private final int maxRetentionTimeInMs = 5000;
    private ReplicatedStoragesClusterEnv environment;

    @BeforeAll
    void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new ReplicatedStoragesClusterEnv().setMaxRetention(maxRetentionTimeInMs).create();
        environment.await(15000);
        environment.awaitLeader(30000);
    }

    @AfterAll
    void teardown() {
        environment.destroy();
    }


    @Test
    @Order(1)
    @DisplayName("Setup values for storages")
    void test_1() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();
        var asStorage = environment.getAsStorage();

        euStorage.set("one", 1);
        usStorage.set("two", 2);
        asStorage.set("three", 3);
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(1, usStorage.get("one"));
        Assertions.assertEquals(1, asStorage.get("one"));
        Assertions.assertEquals(2, euStorage.get("two"));
        Assertions.assertEquals(2, asStorage.get("two"));
        Assertions.assertEquals(3, euStorage.get("three"));
        Assertions.assertEquals(3, usStorage.get("three"));
    }

    @Test
    @Order(2)
    @DisplayName("Detach and attach euGrid and add new items so when it comes back it needs to sync")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();
        var asStorage = environment.getAsStorage();

        this.environment.detachEuWest(30000);
        this.environment.awaitLeader(30000);

        Thread.sleep(2 * this.maxRetentionTimeInMs);
        usStorage.set("four", 4);
        asStorage.set("five", 5);
        Thread.sleep(2 * this.maxRetentionTimeInMs);
        usStorage.set("six", 6);
        asStorage.set("seven", 7);

        environment.joinEuWest(30000);
        environment.awaitLeader(30000);
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(7, usStorage.localSize());
        Assertions.assertEquals(7, euStorage.localSize());
        Assertions.assertEquals(7, asStorage.localSize());
    }

    @Test
    @Order(3)
    @DisplayName("Detach and attach usEast and add new items so when it comes back it needs to sync")
    void test_3() throws ExecutionException, InterruptedException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();
        var asStorage = environment.getAsStorage();

        this.environment.detachUsEast(30000);
        this.environment.awaitLeader(30000);

        Thread.sleep(2 * this.maxRetentionTimeInMs);
        euStorage.set("eight", 8);
        asStorage.set("nine", 9);
        Thread.sleep(2 * this.maxRetentionTimeInMs);
        euStorage.set("ten", 10);
        asStorage.set("eleven", 11);

        environment.joinUsEast(30000);
        environment.awaitLeader(30000);
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(11, usStorage.localSize());
        Assertions.assertEquals(11, euStorage.localSize());
        Assertions.assertEquals(11, asStorage.localSize());
    }

    @Test
    @Order(4)
    @DisplayName("Detach and attach asEast and add new items so when it comes back it needs to sync")
    void test_4() throws ExecutionException, InterruptedException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();
        var asStorage = environment.getAsStorage();

        this.environment.detachAsEast(30000);
        this.environment.awaitLeader(30000);

        Thread.sleep(2 * this.maxRetentionTimeInMs);
        usStorage.set("twelve", 12);
        euStorage.set("thirteen", 13);
        Thread.sleep(2 * this.maxRetentionTimeInMs);
        usStorage.set("fourteen", 14);
        euStorage.set("fifteen", 15);

        environment.joinAsEast(30000);
        environment.awaitLeader(30000);
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(15, usStorage.localSize());
        Assertions.assertEquals(15, euStorage.localSize());
        Assertions.assertEquals(15, asStorage.localSize());
    }
}