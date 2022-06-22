package com.balazskreith.vstorage.rxutils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

class RxEmitterTest {

    @Test
    void shouldEmitItems_1() throws ExecutionException, InterruptedException, TimeoutException {
        var collector = RxEmitter.<Integer>builder().withMaxTimeInMs(100).build();
        var list = new LinkedList<Integer>();
        collector.subscribe(list::add);

        IntStream.range(0, 10).forEach(collector::accept);
        Thread.sleep(1000);

        IntStream.range(0, 10).forEach(i -> Assertions.assertEquals(i, list.get(i)));
    }

    @Test
    void shouldEmitItems_2() throws Throwable {
        var collector = RxEmitter.<Integer>builder().withMaxTimeInMs(100).build();
        var list = new LinkedList<Integer>();
        collector.subscribe(list::add);

        collector.onNext(List.of(0, 1, 2, 3));
        Thread.sleep(1000);

        IntStream.range(0, 4).forEach(i -> Assertions.assertEquals(i, list.get(i)));
    }

    @Test
    void shouldBeFlushed_1() throws Throwable {
        var collector = RxEmitter.<Integer>builder().withMaxTimeInMs(10000).build();
        var list = new LinkedList<Integer>();
        collector.subscribe(list::add);

        IntStream.range(0, 4).forEach(collector::accept);
        collector.onComplete();

        IntStream.range(0, 4).forEach(i -> Assertions.assertEquals(i, list.get(i)));
    }

    @Test
    void shouldNotEmitAfterFlush() throws Throwable {
        var collector = RxEmitter.<Integer>builder().withMaxTimeInMs(100).build();
        var list = new LinkedList<Integer>();
        collector.subscribe(list::add);

        collector.onComplete();
        collector.accept(1);
        Thread.sleep(1000);

        Assertions.assertEquals(0, list.size());
    }


}