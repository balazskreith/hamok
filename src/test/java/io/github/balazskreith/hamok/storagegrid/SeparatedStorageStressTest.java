package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class SeparatedStorageStressTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageStressTest.class);

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
    void shouldNotCrash() {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        for (var i = 0; i < 10; ++i) {
            var entries_1 = getRandomEntries(1000);
            var entries_2 = getRandomEntries(1000);

            euStorage.setAll(entries_1);
            usStorage.setAll(entries_2);

            usStorage.getAll(entries_1.keySet());
            usStorage.getAll(entries_2.keySet());
        }
        logger.info("End");
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