package io.github.balazskreith.hamok.memorystorages;

import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MemoryStorageBuilderTest {

    @Test
    void shouldBuildMemoryStorage() {
        var storage = new MemoryStorageBuilder<Integer, String>()
                .build();

        Assertions.assertTrue(storage instanceof MemoryStorage<Integer, String>);
    }

    @Test
    void shouldBuildConcurrentMemoryStorage() {
        var storage = new MemoryStorageBuilder<Integer, String>()
                .setConcurrency(true)
                .build();

        Assertions.assertTrue(storage instanceof ConcurrentMemoryStorage<Integer, String>);
    }

    @Test
    void shouldBuildTimeLimitedMemoryStorage() {
        var storage = new MemoryStorageBuilder<Integer, String>()
                .setExpiration(100)
                .build();

        Assertions.assertTrue(storage instanceof TimeLimitedMemoryStorage<Integer, String>);
    }

    @Test
    void shouldBuildConcurrentTimeLimitedMemoryStorage() {
        var storage = new MemoryStorageBuilder<Integer, String>()
                .setConcurrency(true)
                .setExpiration(100)
                .build();

        Assertions.assertTrue(storage instanceof ConcurrentTimeLimitedMemoryStorage<Integer, String>);
    }

    @Test
    void shouldNotBuildTimeLimitedMemoryStorage() {
        Assertions.assertThrows(Exception.class, () -> {
             new MemoryStorageBuilder<Integer, String>()
                    .setExpiration(100, Schedulers.computation())
                    .build();
        });
    }

}