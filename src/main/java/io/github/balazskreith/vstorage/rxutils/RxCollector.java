package io.github.balazskreith.vstorage.rxutils;

import io.github.balazskreith.vstorage.common.InvalidConfigurationException;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class RxCollector<T> extends Observable<List<T>> implements Observer<T>, Consumer<List<T>> {

    private static final Logger logger = LoggerFactory.getLogger(RxCollector.class);

    public static<U> Builder<U> builder() {
        return new Builder<>();
    }

    private AtomicLong emitted = new AtomicLong(0);
    private AtomicLong started = new AtomicLong(RxBuffer.NOT_STARTED);
    private final CompositeDisposable disposer = new CompositeDisposable();

    private Subject<List<T>> output = PublishSubject.create();
    private LinkedBlockingQueue<T> buffer = new LinkedBlockingQueue<>();
    private int maxItems = 0;
    private Timeout timeout;

    private RxCollector() {
        this.disposer.add(Disposable.fromRunnable(() -> {
            this.buffer.clear();
            this.timeout.stop();
            this.emitted.set(0);
            this.started.set(RxBuffer.NOT_STARTED);
        }));
    }

    /**
     * The actual number of items collected in this element
     * @return
     */
    public int size() {
        return this.buffer.size();
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super List<T>> observer) {
        this.output.subscribe(observer);
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
        // if the output is finished we do not accept new input
        if (this.isTerminated()) {
            d.dispose();
            return;
        }
        this.disposer.add(d);
    }

    @Override
    public void onNext(@NonNull T item) {
        this.start();

        this.buffer.add(item);

        if (this.shouldEmit()) {
            this.emit();
        }
    }

    @Override
    public void onError(@NonNull Throwable e) {
        if (this.isTerminated()) {
            return;
        }
        this.output.onError(e);
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
    }

    @Override
    public void onComplete() {
        if (this.isTerminated()) {
            return;
        }
        this.emit();
        this.output.onComplete();
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
    }

    public boolean isTerminated() {
        return this.output.hasComplete() || this.output.hasThrowable();
    }

    @Override
    public void accept(List<T> items) throws Throwable {
        this.start();

        this.buffer.addAll(items);

        if (this.shouldEmit()) {
            this.emit();
        }
    }

    private void emit() {
        List<T> collectedItems;
        if (this.buffer.size() < 1) {
            return;
        }
        collectedItems = new LinkedList<>();
        this.buffer.drainTo(collectedItems);
        if (collectedItems.size() < 1) {
            return;
        }
        synchronized (this) {
            this.output.onNext(collectedItems);
        }
        this.started.set(RxBuffer.NOT_STARTED);
        this.emitted.set(Instant.now().toEpochMilli());
    }

    RxBuffer toRxBuffer() {
        var collector = this;
        return new RxBuffer() {
            @Override
            public int size() {
                return collector.size();
            }

            @Override
            public void emit() {
                collector.emit();
            }

            @Override
            public long started() {
                return collector.started.get();
            }

            @Override
            public long emitted() {
                return collector.emitted.get();
            }

            @Override
            public boolean isDisposed() {
                return collector.isTerminated();
            }
        };
    }

    private void start() {
        if (!this.started.compareAndSet(RxBuffer.NOT_STARTED, Instant.now().toEpochMilli())) {
            return;
        }
        this.timeout.start();
    }

    private boolean shouldEmit() {
        return 0 < this.maxItems && this.maxItems <= this.buffer.size();
    }

    public static class Builder<U> {
        private long maxTimeInMs = 0;
        private RxCollector<U> result = new RxCollector<>();
        private Consumer<RxCollector<U>> callback;
        public Builder<U> withMaxItems(int value) {
            this.result.maxItems = value;
            return this;
        }
        public Builder<U> withMaxTimeInMs(long value) {
            this.maxTimeInMs = value;
            return this;
        }

        public Builder<U> withTimeout(Timeout timeout) {
            this.result.timeout = timeout;
            return this;
        }

        Builder<U> onBuilt(Consumer<RxCollector<U>> callback) {
            this.callback = callback;
            return this;
        }

        public RxCollector<U> build() {
            if (this.result.maxItems < 1 && this.maxTimeInMs < 1) {
                throw new IllegalStateException("Collector must be set to hold max items or max time");
            }
            if (0 < this.maxTimeInMs && this.result.timeout != null && this.result.timeout.getTimeoutInMs() != this.maxTimeInMs) {
                throw new InvalidConfigurationException("Mismatching timeouts! The timeout set for the collector is " + this.result.timeout.getTimeoutInMs() + " in ms and the builder has a timeout controller, which has " + this.maxTimeInMs + " timeout in ms");
            }
            if (this.maxTimeInMs < 1) {
                this.result.timeout = Timeout.createVoidTimeout();
            } else if (this.result.timeout == null) {
                this.result.timeout = new TimeoutController(this.maxTimeInMs).addRxCollector(this.result);
            }

            try {
                return this.result;
            } finally {
                if (this.callback != null) {
                    try {
                        this.callback.accept(this.result);
                    } catch (Throwable e) {
                        logger.warn("Error occurred while calling build callback", e);
                    }
                    this.callback = null;
                }
            }
        }
    }
}
