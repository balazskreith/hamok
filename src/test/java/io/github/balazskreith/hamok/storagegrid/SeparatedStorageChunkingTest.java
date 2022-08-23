package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SeparatedStorageChunkingTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageChunkingTest.class);

    private SeparatedStoragesEnv environment;

    @BeforeAll
    void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new SeparatedStoragesEnv()
                .setRequestMessageLimits(100, 10, collidingEntries -> {
                    logger.warn("Colliding entries {}", collidingEntries);
                })
                .create();
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

        for (var i = 0; i < 100; ++i) {
            var entries_1 = getRandomEntries(1000);
            var entries_2 = getRandomEntries(1000);

            euStorage.setAll(entries_1);
            usStorage.setAll(entries_2);

            euStorage.getAll(entries_2.keySet());
            usStorage.getAll(entries_1.keySet());
        }
    }

    private Map<String, Integer> getRandomEntries(int length) {
        var random = new Random();
        var result = new HashMap<String, Integer>();
        for (var i = 0; i < length; ++i) {
            var value = random.nextInt();
            result.put(String.valueOf(value), value);
        }
        return result;
    }
}