package io.github.balazskreith.hamok.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

class UtilsTest {

    @Test
    void testFirstNonNull() {
        var first = Utils.firstNonNull(1, 2);
        var second = Utils.firstNonNull(null, 2);

        Assertions.assertEquals(1, first);
        Assertions.assertEquals(2, second);
    }

    @Test
    void testSupplyIfTrue() {
        var first = Utils.supplyIfTrue(true, () -> 1);
        var second = Utils.supplyIfTrue(false, () -> 1);

        Assertions.assertEquals(1, first);
        Assertions.assertEquals(null, second);
    }

    @Test
    void testSupplyIfTrueOrElse() {
        var first = Utils.supplyIfTrueOrElse(true, () -> 1, 2);
        var second = Utils.supplyIfTrueOrElse(false, () -> 1, 2);

        Assertions.assertEquals(1, first);
        Assertions.assertEquals(2, second);
    }

    @Test
    void testSupplyMappedIfTrue() {
        var first = Utils.supplyMappedIfTrue(true, () -> "1", Integer::parseInt);
        var second = Utils.supplyMappedIfTrue(false, () -> "1", Integer::parseInt);

        Assertions.assertEquals(1, first);
        Assertions.assertEquals(null, second);
    }

    @Test
    void testSupplyStringToUuidIfTrue() {
        UUID expected = UUID.randomUUID();
        var actual = Utils.supplyStringToUuidIfTrue(true, () -> expected.toString());

        Assertions.assertTrue(UuidTools.equals(actual, expected));
    }

    @Test
    void testRelayIfNotNull_1() {
        var result = new AtomicInteger(0);
        Utils.relayIfNotNull(() -> 1, result::set);

        Assertions.assertEquals(result.get(), 1);
    }

    @Test
    void testRelayIfNotNull_2() {
        var result = new AtomicInteger(0);
        Utils.relayIfNotNull(() -> null, result::set);

        Assertions.assertEquals(result.get(), 0);
    }

    @Test
    void testRelayMappedIfNotNull_1() {
        var result = new AtomicInteger(0);
        Utils.relayMappedIfNotNull(() -> "1", Integer::parseInt, result::set);

        Assertions.assertEquals(result.get(), 1);
    }

    @Test
    void testRelayMappedIfNotNull_2() {
        var result = new AtomicInteger(0);
        Utils.<String, Integer>relayMappedIfNotNull(() -> null, Integer::parseInt, result::set);

        Assertions.assertEquals(result.get(), 0);
    }

}