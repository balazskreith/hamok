package io.github.balazskreith.vstorage.rxutils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class RxAtomicReferenceTest {

    @Test
    void shouldHaveTheNewValue() {
        var reference = new RxAtomicReference<Integer>(-1);
        reference.set(1);

        Assertions.assertEquals(1, reference.get());
    }

    @Test
    void shouldEmitTheNewValue() {
        var actual = new AtomicReference<Optional<Integer>>(Optional.empty());
        var reference = new RxAtomicReference<Integer>(-1);
        reference.subscribe(actual::set);

        reference.set(1);
        Assertions.assertEquals(reference.get(), actual.get().get());
    }

    @Test
    void shouldOnlyNotifyOnNewValue() {
        var changed = new AtomicInteger(0);
        var reference = new RxAtomicReference<Integer>(-1);
        reference.subscribe(value -> changed.incrementAndGet());

        reference.set(2); // changed here
        reference.set(2);
        reference.set(3); // changed here
        reference.set(3);
        reference.set(2); // changed here

        Assertions.assertEquals(3, changed.get());
    }
}