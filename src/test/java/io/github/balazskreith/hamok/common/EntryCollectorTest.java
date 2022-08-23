package io.github.balazskreith.hamok.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

class EntryCollectorTest {

    @Test
    void shouldHaveAllValues() {
        var map_1 = Map.of("one", 1);
        var map_2 = Map.of("two", 2);

        var subject = Stream.concat(map_1.entrySet().stream(), map_2.entrySet().stream())
                .collect(EntryCollector.create());

        Assertions.assertEquals(2, subject.size());
        Assertions.assertEquals(1, subject.get("one"));
        Assertions.assertEquals(2, subject.get("two"));
    }

    @Test
    void shouldDetectCollidingValues() throws ExecutionException, InterruptedException, TimeoutException {
        var map_1 = Map.of("one", 1);
        var map_2 = Map.of("one", 2);

        var detectedCollision = new AtomicReference<DetectedEntryCollision<String, Integer>>(null);
        var subject = Stream.concat(map_1.entrySet().stream(), map_2.entrySet().stream())
                .collect(EntryCollector.create(null, detectedCollision::set));

        Assertions.assertEquals(1, subject.size());
        Assertions.assertEquals("one", detectedCollision.get().key());
        Assertions.assertEquals(3, detectedCollision.get().value1() + detectedCollision.get().value2());
        Assertions.assertEquals(2, subject.get("one"));
    }
}