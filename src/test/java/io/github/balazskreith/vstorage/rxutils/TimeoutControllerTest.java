package io.github.balazskreith.vstorage.rxutils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class TimeoutControllerTest {

    @Test
    void shouldTimeoutRunnable_1() throws ExecutionException, InterruptedException, TimeoutException {
        var future = new CompletableFuture<Boolean>();
        var timeout = new TimeoutController(100).addRunnable(() -> future.complete(true));
        timeout.start();
        Assertions.assertTrue(future.get(1000, TimeUnit.MILLISECONDS));
    }

    @Test
    void shouldTimeoutSeveralCollector_1() throws ExecutionException, InterruptedException, TimeoutException {
        var timeout = new TimeoutController(100);
        var collector_1 = timeout.<Integer>rxCollectorBuilder().withMaxItems(100).build();
        var collector_2 = timeout.<Integer>rxCollectorBuilder().withMaxItems(100).build();
        var future_1 = new CompletableFuture<List<Integer>>();
        var future_2 = new CompletableFuture<List<Integer>>();

        collector_1.subscribe(future_1::complete);
        collector_2.subscribe(future_2::complete);
        collector_1.onNext(1);
        collector_2.onNext(2);

        CompletableFuture.allOf(future_1, future_2).get(1000, TimeUnit.MILLISECONDS);

        Assertions.assertEquals(1, future_1.get().get(0));
        Assertions.assertEquals(2, future_2.get().get(0));
    }

    @Test
    void shouldTimeoutSeveralEmitter_1() throws ExecutionException, InterruptedException, TimeoutException {
        var timeout = new TimeoutController(100);
        var emitter_1 = timeout.<Integer>rxEmitterBuilder().withMaxItems(100).build();
        var emitter_2 = timeout.<Integer>rxEmitterBuilder().withMaxItems(100).build();
        var future_1 = new CompletableFuture<Integer>();
        var future_2 = new CompletableFuture<Integer>();

        emitter_1.subscribe(future_1::complete);
        emitter_2.subscribe(future_2::complete);
        emitter_1.accept(1);
        emitter_2.accept(2);

        CompletableFuture.allOf(future_1, future_2).get(1000, TimeUnit.MILLISECONDS);

        Assertions.assertEquals(1, future_1.get());
        Assertions.assertEquals(2, future_2.get());
    }
}