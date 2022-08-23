package io.github.balazskreith.hamok.common;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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

    public static<T> T supplyIfTrue(boolean condition, Supplier<T> supplier) {
        if (condition) return supplier.get();
        else return null;
    }

    public static<T> T supplyIfTrueOrElse(boolean condition, Supplier<T> supplier, T defaultValue) {
        if (condition) return supplier.get();
        else return defaultValue;
    }

    public static<U, R> R supplyMappedIfTrue(boolean condition, Supplier<U> supplier, Function<U, R> mapper) {
        if (!condition) return null;
        var input = supplier.get();
        if (input == null) return null;
        return mapper.apply(input);
    }

    public static UUID supplyStringToUuidIfTrue(boolean condition, Supplier<String> supplier) {
        return supplyMappedIfTrue(condition, supplier, UUID::fromString);
    }

    public static<T> void relayIfNotNull(Supplier<T> source, Consumer<T> consumer) {
        var value = source.get();
        if (value != null) consumer.accept(value);
    }

    public static<U, R> void relayMappedIfNotNull(Supplier<U> source, Function<U, R> mapper, Consumer<R> consumer) {
        var input = source.get();
        if (input == null) return;
        var output = mapper.apply(input);
        if (output == null) return;
        consumer.accept(output);
    }


    public static void relayUuidToStringIfNotNull(Supplier<UUID> source, Consumer<String> consumer) {
        Utils.<UUID, String>relayMappedIfNotNull(source, UUID::toString, consumer);
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
