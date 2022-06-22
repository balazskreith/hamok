package com.balazskreith.vstorage.common;

import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

class RwLockTest {

    @Test
    void shouldNotReadWhileWrite_1() throws ExecutionException, InterruptedException {
        var rwLock = new RwLock();
        var started = Instant.now().toEpochMilli();
        var ended = new CompletableFuture<Long>();
        Schedulers.single().createWorker().schedule(() -> {
//            System.out.println("scheduled reading");
            rwLock.runInReadLock(() -> {
//                System.out.println("started reading");
                ended.complete(Instant.now().toEpochMilli());
            });
        }, 500, TimeUnit.MILLISECONDS);
        rwLock.runInWriteLock(() -> {
//            System.out.println("started writing");
            sleep(2000);
//            System.out.println("ended writing");
        });
        var elapsed = ended.get() - started;
        Assertions.assertTrue(1000 < elapsed);
        Assertions.assertTrue(elapsed < 5000);
    }

    @Test
    void shouldNotReadWhileWrite_2() throws ExecutionException, InterruptedException {
        var rwLock = new RwLock();
        var started = Instant.now().toEpochMilli();
        var ended = new CompletableFuture<Long>();
        Schedulers.single().createWorker().schedule(() -> {
//            System.out.println("scheduled reading");
            var ts = rwLock.supplyInReadLock(() -> {
//                System.out.println("started reading");
                return Instant.now().toEpochMilli();
            });
            ended.complete(ts);
        }, 500, TimeUnit.MILLISECONDS);
        rwLock.supplyInWriteLock(() -> {
//            System.out.println("started writing");
            sleep(2000);
            return null;
//            System.out.println("ended writing");
        });
        var elapsed = ended.get() - started;
        Assertions.assertTrue(1000 < elapsed);
        Assertions.assertTrue(elapsed < 5000);
    }

    void sleep(int timeoutInMs) {
        try {
            Thread.sleep(timeoutInMs);
        } catch (InterruptedException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}