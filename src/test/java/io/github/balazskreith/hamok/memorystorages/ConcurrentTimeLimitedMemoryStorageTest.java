package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.ModifiedStorageEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class ConcurrentTimeLimitedMemoryStorageTest {

    @Test
    void shouldReturnWithSize() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.set(1, "one");
        Assertions.assertEquals(1, storage.size());
    }

    @Test
    void shouldClear() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.set(1, "one");
        storage.clear();
        Assertions.assertEquals(0, storage.size());
    }

    @Test
    void shouldHaveKeys() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.set(1, "one");
        var keys = storage.keys();
        Assertions.assertTrue(keys.contains(1));
    }

    @Test
    void shouldBeEmpty() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        Assertions.assertTrue(storage.isEmpty());
    }

    @Test
    void shouldBeNotEmpty() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.set(1, "one");
        Assertions.assertFalse(storage.isEmpty());
    }

    @Test
    void shouldPutAndGetAll() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.setAll(Map.of(1, "one", 2, "two"));
        var entries = storage.getAll(Set.of(1, 2));
        Assertions.assertEquals("one", entries.get(1));
        Assertions.assertEquals("two", entries.get(2));
    }

    @Test
    void shouldPutAndDelete() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.set(1, "one");
        storage.delete(1);
        Assertions.assertNull(storage.get(1));
    }

    @Test
    void shouldPutAndDeleteAll() {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.setAll(Map.of(1, "one", 2, "two"));
        storage.deleteAll(Set.of(1, 2));
        Assertions.assertEquals(0, storage.getAll(Set.of(1, 2)).size());
    }

    @Test
    void shouldClose() throws Exception {
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.setAll(Map.of(1, "one", 2, "two"));
        storage.close();
        Assertions.assertEquals(0, storage.size());
    }

    @Test
    void shouldNotifyByCreation_1() throws ExecutionException, InterruptedException, TimeoutException {
        var createdEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().createdEntry().subscribe(createdEntry::complete);

        storage.set(1, "one");

        var modifiedEntry = createdEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getNewValue());
    }

    @Test
    void shouldNotifyByCreation_2() throws ExecutionException, InterruptedException, TimeoutException {
        var createdEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().createdEntry().subscribe(createdEntry::complete);

        storage.setAll(Map.of(1, "one"));

        var modifiedEntry = createdEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getNewValue());
    }

    @Test
    void shouldNotifyByUpdated_1() throws ExecutionException, InterruptedException, TimeoutException {
        var updatedEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().updatedEntry().subscribe(updatedEntry::complete);

        storage.set(1, "one");
        storage.set(1, "two");

        var modifiedEntry = updatedEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
        Assertions.assertEquals("two", modifiedEntry.getNewValue());
    }

    @Test
    void shouldNotifyByUpdated_2() throws ExecutionException, InterruptedException, TimeoutException {
        var updatedEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().updatedEntry().subscribe(updatedEntry::complete);

        storage.setAll(Map.of(1, "one"));
        storage.setAll(Map.of(1, "two"));

        var modifiedEntry = updatedEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
        Assertions.assertEquals("two", modifiedEntry.getNewValue());
    }

    @Test
    void shouldNotifyByDeleted_1() throws ExecutionException, InterruptedException, TimeoutException {
        var deletedEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().deletedEntry().subscribe(deletedEntry::complete);

        storage.set(1, "one");
        storage.delete(1);

        var modifiedEntry = deletedEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
    }

    @Test
    void shouldNotifyByDeleted_2() throws ExecutionException, InterruptedException, TimeoutException {
        var deletedEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().deletedEntry().subscribe(deletedEntry::complete);

        storage.setAll(Map.of(1, "one"));
        storage.deleteAll(Set.of(1));

        var modifiedEntry = deletedEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
    }

    @Test
    void shouldNotifyByEvicted_1() throws ExecutionException, InterruptedException, TimeoutException {
        var evictedEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(1000);
        storage.events().evictedEntry().subscribe(evictedEntry::complete);

        storage.set(1, "one");
        storage.clear();

        var modifiedEntry = evictedEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
    }


    @Test
    void shouldNotifyByExpired_1() throws ExecutionException, InterruptedException, TimeoutException {
        var expiredEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(100);
        storage.events().expiredEntry().subscribe(expiredEntry::complete);

        storage.set(1, "one");
        Thread.sleep(500);

        var modifiedEntry = expiredEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
    }

    @Test
    void shouldNotifyByExpired_2() throws ExecutionException, InterruptedException, TimeoutException {
        var expiredEntry = new CompletableFuture<ModifiedStorageEntry<Integer, String>>();
        var storage = new ConcurrentTimeLimitedMemoryStorage<Integer, String>(100);
        storage.events().expiredEntry().subscribe(expiredEntry::complete);

        storage.setAll(Map.of(1, "one"));
        Thread.sleep(500);

        var modifiedEntry = expiredEntry.get(1000, TimeUnit.MILLISECONDS);
        Assertions.assertEquals(1, modifiedEntry.getKey());
        Assertions.assertEquals("one", modifiedEntry.getOldValue());
    }
}