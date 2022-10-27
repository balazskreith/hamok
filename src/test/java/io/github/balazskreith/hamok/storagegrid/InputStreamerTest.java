package io.github.balazskreith.hamok.storagegrid;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

class InputStreamerTest {

    @Test
    void shouldStreamKeysOnce() {
        var appearances = new HashMap<String, Integer>();
        var streamer = new InputStreamer<String, Integer>(1, 1);
        streamer.streamKeys(Set.of("one", "two", "three"))
                .flatMap(s -> s.stream())
                .forEach(key -> {
//                    System.out.println(key);
                    appearances.put(key, appearances.getOrDefault(key, 0) + 1);
                });

        Assertions.assertEquals(1, appearances.get("one"));
        Assertions.assertEquals(1, appearances.get("two"));
        Assertions.assertEquals(1, appearances.get("three"));

    }

    @Test
    void shouldStreamEntriesOnce() {
        var appearances = new HashMap<String, Integer>();
        var streamer = new InputStreamer<String, Integer>(1, 1);
        streamer.streamEntries(Map.of("one", 1, "two", 2, "three", 3))
                .flatMap(s -> s.entrySet().stream())
                .forEach(entry -> {
//                    System.out.println(key);
                    appearances.put(entry.getKey(), appearances.getOrDefault(entry.getKey(), 0) + 1);
                });

        Assertions.assertEquals(1, appearances.get("one"));
        Assertions.assertEquals(1, appearances.get("two"));
        Assertions.assertEquals(1, appearances.get("three"));
    }

    @Test
    void shouldSteamEntryWithCorrespondentValue() {
        var entries = new HashMap<String, Integer>();
        var streamer = new InputStreamer<String, Integer>(1, 1);
        streamer.streamEntries(Map.of("one", 1, "two", 2, "three", 3))
                .flatMap(s -> s.entrySet().stream())
                .forEach(entry -> entries.put(entry.getKey(), entry.getValue()));

        Assertions.assertEquals(1, entries.get("one"));
        Assertions.assertEquals(2, entries.get("two"));
        Assertions.assertEquals(3, entries.get("three"));
    }
}