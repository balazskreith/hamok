package io.github.balazskreith.hamok.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * Collect a stream of items into a chunks and forward the collected chunks as list to a dedicated consumer.
 *
 * Handy if you have a very-large stream of items, where even the collection takes time and sill the memory,
 * so by using chunks you can process it much more safely and efficiently
 * @param <T>
 */
public abstract class BatchCollector<T, R extends Collection<T>> implements Collector<T, R, R> {

    public static final Logger logger = LoggerFactory.getLogger(BatchCollector.class);

    public static<K, U extends Collection<K>> Builder<K, U> builder() {
        return new Builder<K, U>();
    }


    public static<K, V> Stream<Map<K, V>> batchedStream(Map<K, V> source, int batchSize) {
        if (source == null) {
            return Stream.empty();
        }
        if (source.size() < batchSize) {
            return Stream.of(source);
        }
        var result = Stream.<Map<K, V>>builder();
        var collector = BatchCollector.<Map.Entry<K, V>, Set<Map.Entry<K, V>>>builder().withBatchSize(batchSize)
                .withEmptyCollectionSupplier(Collections::emptySet)
                .withMutableCollectionSupplier(HashSet::new)
                .withConsumer(batch -> {
                    var entryBatch = batch.stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue
                    ));
                    result.add(entryBatch);
                }).build();
        source.entrySet().stream().collect(collector);
        return result.build();
    }

    /**
     * Creates a new batch collector
     *
     * @param batchSize      the batch size after which the batchProcessor should be called
     * @param batchProcessor the batch processor which accepts batches of records to process
     * @param <T>            the type of elements being processed
     * @return a batch collector instance
     */
    public static <T, R extends Collection<T>> Collector<T, R, R> makeCollector(int batchSize, Consumer<R> batchProcessor) {
        return BatchCollector.<T, R>builder()
                .withBatchSize(batchSize)
                .withConsumer(batchProcessor)
                .build();
    }

    private final int batchSize;
    private final Consumer<R> batchProcessor;


    /**
     * Constructs the batch collector
     *
     * @param batchSize      the batch size after which the batchProcessor should be called
     * @param batchProcessor the batch processor which accepts batches of records to process
     */
    BatchCollector(int batchSize, Consumer<R> batchProcessor) {
        batchProcessor = requireNonNull(batchProcessor);

        this.batchSize = batchSize;
        this.batchProcessor = batchProcessor;
    }

    public abstract Supplier<R> supplier();

    public BiConsumer<R, T> accumulator() {
        return (ts, t) -> {
            ts.add(t);
            if (ts.size() >= batchSize) {
                batchProcessor.accept(ts);
                ts.clear();
            }
        };
    }

    protected abstract R getEmptyCollection();

    public BinaryOperator<R> combiner() {
        return (ts, ots) -> {
            // process each parallel list without checking for batch size
            // avoids adding all elements of one to another
            // can be modified if a strict batching mode is required
            batchProcessor.accept(ts);
            batchProcessor.accept(ots);
            return this.getEmptyCollection();
        };
    }

    public Function<R, R> finisher() {
        return ts -> {
            if (0 < ts.size()) {
                batchProcessor.accept(ts);
            }
            return this.getEmptyCollection();
        };
    }

    public Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }

    public static class Builder<U, K extends Collection<U>> {
        private Integer batchSize = null;
        private Consumer<K> batchProcessor = null;
        private Supplier<K> emptyCollectionSupplier = null;
        private Supplier<K> mutableCollectionSupplier = null;

        private Builder() {

        }

        public Builder<U, K> withEmptyCollectionSupplier(Supplier<K> emptyCollectionSupplier) {
            this.emptyCollectionSupplier = emptyCollectionSupplier;
            return this;
        }

        public Builder<U, K> withMutableCollectionSupplier(Supplier<K> mutableCollectionSupplier) {
            this.mutableCollectionSupplier = mutableCollectionSupplier;
            return this;
        }

        public Builder<U, K> withBatchSize(int value) {
            this.batchSize = value;
            return this;
        }

        public Builder<U, K> withConsumer(Consumer<K> consumer) {
            this.batchProcessor = consumer;
            return this;
        }

        public BatchCollector<U, K> build() {
            Objects.requireNonNull(this.emptyCollectionSupplier);
            Objects.requireNonNull(this.mutableCollectionSupplier);
            Objects.requireNonNull(this.batchProcessor);
            Objects.requireNonNull(this.batchSize);
            return new BatchCollector<>(this.batchSize, this.batchProcessor) {

                @Override
                public Supplier<K> supplier() {
                    return mutableCollectionSupplier;
                }

                @Override
                protected K getEmptyCollection() {
                    return emptyCollectionSupplier.get();
                }
            };
        }

    }
}