package io.github.balazskreith.hamok.common;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public final class Utils {
    private static Iterator EMPTY_ITERATOR = new Iterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object next() {
            return null;
        }
    };

    public static<T> Iterator<T> emptyIterator() {
        return EMPTY_ITERATOR;
    }

    public static<T>  T firstNonNull(T... items) {
        if (items == null) return null;
        for (var item : items) {
            if (item == null) continue;
            return item;
        }
        return null;
    }

    public static <T> CompletableFuture<T> makeCompletableFuture(Future<T> future) {
        if (future.isDone())
            return transformDoneFuture(future);
        return CompletableFuture.supplyAsync(() -> {
            try {
                if (!future.isDone())
                    awaitFutureIsDoneInForkJoinPool(future);
                return future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                // Normally, this should never happen inside ForkJoinPool
                Thread.currentThread().interrupt();
                // Add the following statement if the future doesn't have side effects
                // future.cancel(true);
                throw new RuntimeException(e);
            }
        });
    }

    // source: https://stackoverflow.com/questions/23301598/transform-java-future-into-a-completablefuture
    private static <T> CompletableFuture<T> transformDoneFuture(Future<T> future) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        T result;
        try {
            result = future.get();
        } catch (Throwable ex) {
            cf.completeExceptionally(ex);
            return cf;
        }
        cf.complete(result);
        return cf;
    }

    private static void awaitFutureIsDoneInForkJoinPool(Future<?> future)
            throws InterruptedException {
        ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker() {
            @Override public boolean block() throws InterruptedException {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            @Override public boolean isReleasable() {
                return future.isDone();
            }
        });
    }
}
