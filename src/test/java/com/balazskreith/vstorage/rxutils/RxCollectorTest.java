package com.balazskreith.vstorage.rxutils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

class RxCollectorTest {

    @Test
    void shouldCollectItems_1() throws ExecutionException, InterruptedException, TimeoutException {
        var collector = RxCollector.<Integer>builder().withMaxTimeInMs(100).build();
        var future = new CompletableFuture<List<Integer>>();
        collector.subscribe(future::complete);

        IntStream.range(0, 10).forEach(collector::onNext);

        var list = future.get(1000, TimeUnit.MILLISECONDS);
        IntStream.range(0, 10).forEach(i -> Assertions.assertEquals(i, list.get(i)));
    }

    @Test
    void shouldCollectItems_2() throws Throwable {
        var collector = RxCollector.<Integer>builder().withMaxTimeInMs(100).build();
        var future = new CompletableFuture<List<Integer>>();
        collector.subscribe(future::complete);

        collector.accept(List.of(0, 1, 2, 3));

        var list = future.get(1000, TimeUnit.MILLISECONDS);
        IntStream.range(0, 4).forEach(i -> Assertions.assertEquals(i, list.get(i)));
    }

    @Test
    void shouldBeFlushed_1() throws Throwable {
        var collector = RxCollector.<Integer>builder().withMaxTimeInMs(10000).build();
        var future = new CompletableFuture<List<Integer>>();
        collector.subscribe(future::complete);

        collector.accept(List.of(0, 1, 2, 3));
        collector.onComplete();

        var list = future.get(1000, TimeUnit.MILLISECONDS);
        IntStream.range(0, 4).forEach(i -> Assertions.assertEquals(i, list.get(i)));
    }

    @Test
    void shouldNotCollectAfterFlush() throws Throwable {
        var collector = RxCollector.<Integer>builder().withMaxTimeInMs(10000).build();
        var future = new CompletableFuture<List<Integer>>();
        collector.subscribe(future::complete);

        collector.onComplete();

        Assertions.assertThrows(Exception.class, () -> {
            future.get(100, TimeUnit.MILLISECONDS);
        });
    }

    @Test
    public void shouldCollectOnce_1() throws Throwable {
        AtomicInteger executed = new AtomicInteger(0);
        var collector = RxCollector.<Integer>builder().withMaxItems(2).withMaxTimeInMs(200).build();

        collector.subscribe(items -> {
            executed.incrementAndGet();
        });

        collector.accept(List.of(1, 2));
        Thread.sleep(500);
        Assertions.assertEquals(1, executed.get());
    }

    @Test
    public void shouldCollectOnce_2() throws Throwable {
        AtomicInteger executed = new AtomicInteger(0);
        var collector = RxCollector.<Integer>builder().withMaxItems(2).withMaxTimeInMs(200).build();

        collector.subscribe(items -> {
            executed.incrementAndGet();
        });

        collector.onNext(1);
        Thread.sleep(500);
        collector.onNext(2);
        Assertions.assertEquals(1, executed.get());
    }
}