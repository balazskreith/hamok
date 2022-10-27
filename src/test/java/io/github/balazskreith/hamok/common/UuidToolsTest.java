package io.github.balazskreith.hamok.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class UuidToolsTest {

    @Test
    void shouldBeEqual_0() {
        UUID source = UUID.randomUUID();
        Assertions.assertTrue(UuidTools.equals(null, null));
    }

    @Test
    void shouldBeEqual_1() {
        UUID source = UUID.randomUUID();
        Assertions.assertTrue(UuidTools.equals(source, source));
    }

    @Test
    void shouldBeEqual_2() {
        UUID source = UUID.randomUUID();
        Assertions.assertTrue(UuidTools.equals(source, UUID.fromString(source.toString())));
    }

    @Test
    void shouldBeNotEqual_0() {
        UUID source = UUID.randomUUID();
        Assertions.assertFalse(UuidTools.equals(source, null));
    }

    @Test
    void shouldBeNotEqual_1() {
        UUID source = UUID.randomUUID();
        Assertions.assertFalse(UuidTools.equals(source, UUID.randomUUID()));
    }
}