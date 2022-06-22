package com.balazskreith.vstorage.common;

import io.reactivex.rxjava3.subjects.PublishSubject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class DisposerTest {



    @Test
    void shouldCallDisposables_1() {
        var disposed = new AtomicBoolean(false);
        Disposer.builder().addDisposable(disposed::get, () -> disposed.set(true)).build().dispose();
        Assertions.assertTrue(disposed.get());
    }

    @Test
    void shouldCallDisposables_2() {
        var last = new AtomicInteger(0);
        var subject = PublishSubject.<Integer>create();
        subject.subscribe(last::set);
        var disposer = Disposer.builder()
                .addSubject(subject)
                .build();

        subject.onNext(1);
        disposer.dispose();
        subject.onNext(2);

        Assertions.assertEquals(1, last.get());
    }

    @Test
    void shouldCallDisposables_3() {
        var last = new AtomicInteger(0);
        var subject = PublishSubject.<Integer>create();
        var disposer = Disposer.builder()
                .addDisposable(subject.subscribe(last::set))
                .build();

        subject.onNext(1);
        disposer.dispose();
        subject.onNext(1);

        Assertions.assertEquals(1, last.get());
    }

    @Test
    void shouldDisposeOnlyOnce_1() {
        var invoked = new AtomicInteger(0);
        var disposer = Disposer.builder().addDisposable(() -> 0 < invoked.get(), () -> invoked.incrementAndGet()).build();

        disposer.dispose();
        disposer.dispose();
        disposer.dispose();

        Assertions.assertEquals(1, invoked.get());
    }

    @Test
    void shouldDisposeAll_1() {
        var invoked = new AtomicInteger(0);
        var disposer = Disposer.builder()
                .addDisposable(() -> false, () -> {
                    throw new RuntimeException("some exception");
                })
                .addDisposable(() -> 0 < invoked.get(), () -> invoked.set(1))
                .build();

        disposer.dispose();

        Assertions.assertEquals(1, invoked.get());
    }

    @Test
    void shouldDisposeAll_2() {
        var invoked = new AtomicInteger(0);
        var disposer = Disposer.builder()
                .addDisposable(() -> 0 < invoked.get(), () -> invoked.set(1))
                .addDisposable(() -> false, () -> {
                    throw new RuntimeException("some exception");
                })
                .build();

        disposer.dispose();

        Assertions.assertEquals(1, invoked.get());
    }

    @Test
    void shouldCallOnComplete_1() {
        var invoked = new AtomicInteger(0);
        var disposer = Disposer.builder()
                .onCompleted(() -> invoked.set(1))
                .build();

        disposer.dispose();

        Assertions.assertEquals(1, invoked.get());
    }
}