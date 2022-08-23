package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@DisplayName("Separated Storage Specifications Test")
class SeparatedStorageDistributedSpecificationTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageDistributedSpecificationTest.class);
    private static SeparatedStoragesEnv environment;

    @BeforeAll
    static void setup() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new SeparatedStoragesEnv().create();
        environment.await();
    }

    @BeforeEach
    void reset() {
        environment.clear();
    }

    @AfterAll
    static void teardown() {
        environment.destroy();
    }

    @Test
    void shouldLocalEmpty_1() {
        environment.getEuStorage().set("one", 1);

        Assertions.assertFalse(environment.getEuStorage().isEmpty());
        Assertions.assertFalse(environment.getEuStorage().localIsEmpty());
        Assertions.assertFalse(environment.getUsStorage().isEmpty());
        Assertions.assertTrue(environment.getUsStorage().localIsEmpty());
    }

    @Test
    void shouldLocalEmpty_2() {
        environment.getUsStorage().set("one", 1);

        Assertions.assertFalse(environment.getUsStorage().isEmpty());
        Assertions.assertFalse(environment.getUsStorage().localIsEmpty());
        Assertions.assertFalse(environment.getEuStorage().isEmpty());
        Assertions.assertTrue(environment.getEuStorage().localIsEmpty());
    }

    @Test
    void shouldHaveLocalSize_1() {
        environment.getEuStorage().set("one", 1);

        Assertions.assertEquals(1, environment.getEuStorage().localSize());
        Assertions.assertEquals(1, environment.getEuStorage().size());
        Assertions.assertEquals(1, environment.getUsStorage().size());
        Assertions.assertEquals(0, environment.getUsStorage().localSize());
    }

    @Test
    void shouldHaveLocalSize_2() {
        environment.getUsStorage().set("one", 1);

        Assertions.assertEquals(0, environment.getEuStorage().localSize());
        Assertions.assertEquals(1, environment.getEuStorage().size());
        Assertions.assertEquals(1, environment.getUsStorage().size());
        Assertions.assertEquals(1, environment.getUsStorage().localSize());
    }

    @Test
    void shouldHaveLocalIterator_1() {
        environment.getEuStorage().set("one", 1);
        var euIterator = environment.getEuStorage().iterator();
        var euLocalIterator = environment.getEuStorage().localIterator();
        var usIterator = environment.getUsStorage().iterator();
        var usLocalIterator = environment.getUsStorage().localIterator();

        Assertions.assertTrue(euIterator.hasNext());
        Assertions.assertTrue(euLocalIterator.hasNext());
        Assertions.assertTrue(usIterator.hasNext());
        Assertions.assertFalse(usLocalIterator.hasNext());

    }

    @Test
    void shouldHaveLocalIterator_2() {
        environment.getUsStorage().set("one", 1);
        var euIterator = environment.getEuStorage().iterator();
        var euLocalIterator = environment.getEuStorage().localIterator();
        var usIterator = environment.getUsStorage().iterator();
        var usLocalIterator = environment.getUsStorage().localIterator();

        Assertions.assertTrue(euIterator.hasNext());
        Assertions.assertFalse(euLocalIterator.hasNext());
        Assertions.assertTrue(usIterator.hasNext());
        Assertions.assertTrue(usLocalIterator.hasNext());
    }

    @Test
    void shouldClearLocally_1() {
        environment.getEuStorage().set("one", 1);
        environment.getUsStorage().set("two", 2);
        environment.getEuStorage().localClear();

        Assertions.assertEquals(1, environment.getUsStorage().localSize());
        Assertions.assertEquals(0, environment.getEuStorage().localSize());
    }

    @Test
    void shouldClearLocally_2() {
        environment.getEuStorage().set("one", 1);
        environment.getUsStorage().set("two", 2);
        environment.getUsStorage().localClear();

        Assertions.assertEquals(0, environment.getUsStorage().localSize());
        Assertions.assertEquals(1, environment.getEuStorage().localSize());
    }
}