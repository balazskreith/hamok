package io.github.balazskreith.hamok.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class SetUtilsTest {

    @Test
    void shouldAddAll_1() {
        var set = SetUtils.<Integer>combineAll(Set.of(1), Set.of(2));

        Assertions.assertTrue(set.contains(1));
        Assertions.assertTrue(set.contains(2));
    }
}