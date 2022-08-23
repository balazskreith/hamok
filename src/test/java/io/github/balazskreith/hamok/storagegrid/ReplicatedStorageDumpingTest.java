package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicatedStorageDumpingTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageDumpingTest.class);

    private final int maxRetentionTimeInMs = 5000;
    private ReplicatedStoragesEnv environment;

    @BeforeAll
    void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new ReplicatedStoragesEnv().setMaxRetention(maxRetentionTimeInMs).create();
        environment.await();
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

        euStorage.set("one", 1);
        usStorage.set("two", 2);
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(1, usStorage.get("one"));
        Assertions.assertEquals(2, euStorage.get("two"));
    }

    @Test
    @Order(2)
    @DisplayName("Detach US East so two storages will be in standalone mode" +
            "")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        environment.detachUsEast(30000);

        Thread.sleep(15000);
        euStorage.set("three", 3);
        usStorage.set("four", 4);

        environment.joinUsEast(30000);

        Thread.sleep(15000);
        environment.awaitLeader(30000);
        environment.awaitUntilCommitsSynced();

        Assertions.assertEquals(4, usStorage.localSize());
        Assertions.assertEquals(4, euStorage.localSize());
    }
}