package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

@DisplayName("Replicated Storage Specifications Test")
class ReplicatedStorageSpecificationTest {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedStorageSpecificationTest.class);

    private StorageGrid grid;
    private ReplicatedStorage<String, Integer> storage;

    @BeforeEach
    void setup() {
        Function<Integer, byte[]> valueEncoder = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> valueDecoder = b -> ByteBuffer.wrap(b).getInt();

        this.grid = StorageGrid.builder().build();
        this.storage = this.grid.<String, Integer>replicatedStorage()
                .setStorageId("replicated-storage-test")
                .setMaxCollectedStorageEvents(1)
                .setMaxCollectedStorageTimeInMs(0)
                .setKeyCodec(str -> str.getBytes(), String::new)
                .setValueCodec(valueEncoder, valueDecoder)
                .build();

    }

    @AfterEach
    void teardown() {

    }

    @Test
    void shouldBeEmpty() {
        Assertions.assertEquals(0, this.storage.localSize());
        Assertions.assertTrue(this.storage.localIsEmpty());
    }

    @Test
    void shouldGetAll() {
        this.storage.set("one", 1);
        var entries = this.storage.getAll(Set.of("one"));

        Assertions.assertEquals(1, entries.get("one"));
    }

    @Test
    void shouldSet() {
        var prevValue = this.storage.set("one", 1);

        Assertions.assertNull(prevValue);
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
    }

    @Test
    void shouldSetAll() {
        var prevEntries = this.storage.setAll(Map.of("one", 1));

        Assertions.assertEquals(0, prevEntries.size());
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
    }

    @Test
    void shouldInsert() {
        var presentedValue = this.storage.insert("one", 1);

        Assertions.assertNull(presentedValue);
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
    }

    @Test
    void shouldInsertAll() {
        var presentedEntries = this.storage.insertAll(Map.of("one", 1));

        Assertions.assertEquals(0, presentedEntries.size());
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
    }

    @Test
    void shouldNotInsert() {
        this.storage.insert("one", 1);
        var presentedValue = this.storage.insert("one", 2);

        Assertions.assertEquals(1, presentedValue);
        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertFalse(this.storage.localIsEmpty());
    }

    @Test
    void shouldNotInsertAll() {
        this.storage.insertAll(Map.of("one", 1));
        var presentedEntries = this.storage.insertAll(Map.of("one", 2, "two", 2));

        Assertions.assertEquals(1, this.storage.get("one"));
        Assertions.assertEquals(2, this.storage.get("two"));
        Assertions.assertEquals(1, presentedEntries.get("one"));
        Assertions.assertEquals(null, presentedEntries.get("two"));
    }

    @Test
    void shouldDelete() {
        this.storage.set("one", 1);
        this.storage.delete("one");

        Assertions.assertNull(this.storage.get("one"));
        Assertions.assertTrue(this.storage.localIsEmpty());
    }

    @Test
    void shouldDeleteAll() {
        this.storage.set("one", 1);
        this.storage.deleteAll(Set.of("one"));

        Assertions.assertNull(this.storage.get("one"));
        Assertions.assertTrue(this.storage.localIsEmpty());
    }

    @Test
    void shouldHaveAllKeys() {
        this.storage.set("one", 1);
        var keys = this.storage.keys();

        Assertions.assertEquals(1, keys.size());
        Assertions.assertTrue(keys.contains("one"));
    }

    @Test
    void shouldHaveLocalKeys() {
        this.storage.set("one", 1);
        var keys = this.storage.localKeys();

        Assertions.assertEquals(1, keys.size());
        Assertions.assertTrue(keys.contains("one"));
    }

    @Test
    void shouldClear() {
        this.storage.set("one", 1);
        this.storage.clear();

        Assertions.assertNull(this.storage.get("one"));
        Assertions.assertTrue(this.storage.localIsEmpty());
        Assertions.assertTrue(this.storage.isEmpty());
    }
}