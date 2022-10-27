package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class SeparatedStorageBackupTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageBackupTest.class);

    private static SeparatedStoragesEnv environment;

    @BeforeAll
    static void init() throws ExecutionException, InterruptedException, TimeoutException {
        environment = new SeparatedStoragesEnv().addBackups(1, 0).create();
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
    void shouldSaveInBackup() throws InterruptedException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);

        // time to send and receive messages
        Thread.sleep(1000);

        Assertions.assertEquals(1, environment.getEuWestBackups().getSavedEntriesSize());
        Assertions.assertEquals(0, environment.getEuWestBackups().getStoredEntriesSize());
        Assertions.assertEquals(0, environment.getUsEastBackups().getSavedEntriesSize());
        Assertions.assertEquals(1, environment.getUsEastBackups().getStoredEntriesSize());
    }

    @Test
    void shouldDeleteFromBackupOnDelete() throws InterruptedException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);
        euStorage.delete("one");

        // time to send and receive messages
        Thread.sleep(1000);

        Assertions.assertEquals(0, environment.getEuWestBackups().getSavedEntriesSize());
        Assertions.assertEquals(0, environment.getEuWestBackups().getStoredEntriesSize());
        Assertions.assertEquals(0, environment.getUsEastBackups().getSavedEntriesSize());
        Assertions.assertEquals(0, environment.getUsEastBackups().getStoredEntriesSize());
    }

    @Test
    void shouldRestoreFromBackup() throws InterruptedException, ExecutionException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);

        try {
            Thread.sleep(1000);

            environment.detachEuWest(10000);

            Thread.sleep(1000);

            Assertions.assertEquals(1, euStorage.get("one"));
            Assertions.assertEquals(1, usStorage.get("one"));
            Assertions.assertEquals(0, environment.getEuWestBackups().getSavedEntriesSize());
            Assertions.assertEquals(0, environment.getEuWestBackups().getStoredEntriesSize());
            Assertions.assertEquals(0, environment.getUsEastBackups().getSavedEntriesSize());
            Assertions.assertEquals(0, environment.getUsEastBackups().getStoredEntriesSize());
        } finally {
            environment.joinEuWest(10000);
        }
    }

    @Test
    void shouldCleanCollidingEntriesAfterRejoin() throws InterruptedException, ExecutionException, TimeoutException {
        var euStorage = environment.getEuStorage();
        var usStorage = environment.getUsStorage();

        euStorage.set("one", 1);

        try {
            Thread.sleep(1000);

            environment.detachEuWest(10000);

            Thread.sleep(1000);

            environment.joinEuWest(10000);

            Thread.sleep(1000);

            Assertions.assertEquals(1, euStorage.get("one"));
            Assertions.assertEquals(1, usStorage.get("one"));
            Assertions.assertEquals(1, euStorage.localSize() + usStorage.localSize());

            Assertions.assertEquals(1, environment.getEuWestBackups().getSavedEntriesSize() + environment.getUsEastBackups().getSavedEntriesSize());
            Assertions.assertEquals(1, environment.getEuWestBackups().getStoredEntriesSize() + environment.getUsEastBackups().getStoredEntriesSize());
        } finally {
            environment.joinEuWest(10000);
        }
    }
}