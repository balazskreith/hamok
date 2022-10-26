package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.ModifiedStorageEntry;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@DisplayName("Separated Storage Specifications Test")
class SeparatedStorageSpecificationTest {

    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageSpecificationTest.class);
    private static final String STORAGE_ID = UUID.randomUUID().toString();
    private StorageGrid grid;
    private SeparatedStorage<String, Integer> storage;

    @BeforeEach
    void setup() {
        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
        Function<String, byte[]> strEnc = s -> s.getBytes();
        Function<byte[], String> strDec = b -> new String(b);
//        BinaryOperator<Integer> mergeOp = (itemsFromStockPile1, itemsFromStockPile2) -> itemsFromStockPile1 + itemsFromStockPile2;

        this.grid = StorageGrid.builder().build();
        this.storage = this.grid.<String, Integer>separatedStorage()
                .setStorageId(STORAGE_ID)
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
                .setKeyCodec(strEnc, strDec)
                .setValueCodec(intEnc, intDec)
                .build();

    }

    @AfterEach
    void teardown() {

    }

    @Test
    void shouldGetId() {
        Assertions.assertEquals(STORAGE_ID, this.storage.getId());
    }

    @Test
    void shouldHaveSize() {
        this.storage.set("one", 1);
        Assertions.assertEquals(1, this.storage.size());
    }

    @Test
    void shouldBeEmpty() {
        Assertions.assertTrue(this.storage.isEmpty());
    }

    @Test
    void shouldClear() {
        this.storage.set("one", 1);
        this.storage.clear();

        Assertions.assertNull(this.storage.get("one"));
        Assertions.assertTrue(this.storage.localIsEmpty());
        Assertions.assertTrue(this.storage.isEmpty());
    }

    @Test
    void shouldHaveAllKeys() {
        this.storage.set("one", 1);
        var keys = this.storage.keys();

        Assertions.assertEquals(1, keys.size());
        Assertions.assertTrue(keys.contains("one"));
    }

    @Test
    void shouldGet() {
        this.storage.set("one", 1);
        var entry = this.storage.get("one");

        Assertions.assertEquals(1, entry);
    }

    @Test
    void shouldGetAll() {
        this.storage.set("one", 1);
        var entries = this.storage.getAll(Set.of("one"));

        Assertions.assertEquals(1, entries.get("one"));
    }

    @Test
    void shouldSet() throws ExecutionException, InterruptedException, TimeoutException {
        var completed = new CompletableFuture<ModifiedStorageEntry<String, Integer>>();
        this.storage.events().createdEntry().subscribe(completed::complete);
        var prevValue = this.storage.set("one", 1);

        Assertions.assertNull(prevValue);
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
        completed.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldSetAll() throws ExecutionException, InterruptedException, TimeoutException {
        var completed = new CompletableFuture<ModifiedStorageEntry<String, Integer>>();
        this.storage.events().createdEntry().subscribe(completed::complete);
        var prevEntries = this.storage.setAll(Map.of("one", 1));

        Assertions.assertEquals(0, prevEntries.size());
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
        completed.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldInsert() throws ExecutionException, InterruptedException, TimeoutException {
        var completed = new CompletableFuture<ModifiedStorageEntry<String, Integer>>();
        this.storage.events().createdEntry().subscribe(completed::complete);
        var presentedValue = this.storage.insert("one", 1);

        Assertions.assertNull(presentedValue);
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
        completed.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldInsertAll() throws ExecutionException, InterruptedException, TimeoutException {
        var completed = new CompletableFuture<ModifiedStorageEntry<String, Integer>>();
        this.storage.events().createdEntry().subscribe(completed::complete);
        var presentedEntries = this.storage.insertAll(Map.of("one", 1));

        Assertions.assertEquals(0, presentedEntries.size());
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
        completed.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldNotInsert() throws ExecutionException, InterruptedException, TimeoutException {
        this.storage.insert("one", 1);
        var presentedValue = this.storage.insert("one", 2);

        Assertions.assertEquals(1, presentedValue);
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
    }

    @Test
    void shouldNotInsertAll() throws ExecutionException, InterruptedException, TimeoutException {
        this.storage.insertAll(Map.of("one", 1));
        var presentedEntries = this.storage.insertAll(Map.of("one", 2, "two", 2));

        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertEquals(2, this.storage.get("two"));
        Assertions.assertEquals(1, presentedEntries.get("one"));
        Assertions.assertEquals(null, presentedEntries.get("two"));
    }

    @Test
    void shouldDelete() throws ExecutionException, InterruptedException, TimeoutException {
        var completed = new CompletableFuture<ModifiedStorageEntry<String, Integer>>();
        this.storage.events().deletedEntry().subscribe(completed::complete);
        this.storage.set("one", 1);
        this.storage.delete("one");

        Assertions.assertNull(this.storage.get("one"));
        Assertions.assertTrue(this.storage.localIsEmpty());
        completed.get(1000, TimeUnit.MILLISECONDS);
    }

    @Test
    void shouldDeleteAll() throws ExecutionException, InterruptedException, TimeoutException {
        var completed = new CompletableFuture<ModifiedStorageEntry<String, Integer>>();
        this.storage.events().deletedEntry().subscribe(completed::complete);
        this.storage.set("one", 1);
        this.storage.deleteAll(Set.of("one"));

        Assertions.assertNull(this.storage.get("one"));
        Assertions.assertTrue(this.storage.localIsEmpty());
        completed.get(1000, TimeUnit.MILLISECONDS);
    }
}