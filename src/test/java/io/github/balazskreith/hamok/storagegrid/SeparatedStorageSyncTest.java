package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SeparatedStorageSyncTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageSyncTest.class);

    private SeparatedStoragesEnv environment;

    @BeforeAll
    void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new SeparatedStoragesEnv().create();
        environment.await();
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

        Assertions.assertEquals(1, usStorage.get("one"));
        Assertions.assertEquals(2, euStorage.get("two"));
    }

    @Test
    @Order(2)
    @DisplayName("Add colliding entries to UsEast")
    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        environment.detachUsEast(30000);
        usStorage.set("one", 1);
        environment.joinUsEast(30000);

        Assertions.assertEquals(2, usStorage.localSize());
        Assertions.assertEquals(1, euStorage.localSize());
    }

    @Test
    @Order(3)
    @DisplayName("Sync storages")
    void test_3() throws ExecutionException, InterruptedException, TimeoutException {

        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.checkCollidingEntries();
        usStorage.checkCollidingEntries();
        environment.awaitLeader(30000);
        environment.getFollowerGrid().sync(100000);

        Assertions.assertEquals(2, usStorage.localSize() + euStorage.localSize());
        Assertions.assertEquals(1, euStorage.get("one"));
        Assertions.assertEquals(2, usStorage.get("two"));
    }
}