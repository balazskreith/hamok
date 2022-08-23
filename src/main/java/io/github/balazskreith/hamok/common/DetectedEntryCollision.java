package io.github.balazskreith.hamok.common;

public record DetectedEntryCollision<K, V>(
        K key,
        V value1,
        V value2
) {
}
