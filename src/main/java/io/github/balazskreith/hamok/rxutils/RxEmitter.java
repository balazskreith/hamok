package io.github.balazskreith.hamok.rxutils;

import io.github.balazskreith.hamok.common.InvalidConfigurationException;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class RxEmitter<T> extends Observable<T> implements Observer<List<T>>, Consumer<T> {

    private static final Logger logger = LoggerFactory.getLogger(RxEmitter.class);

    public static<U> Builder<U> builder() {
        return new Builder<>();
    }

    private AtomicLong emitted = new AtomicLong(0);
    private AtomicLong started = new AtomicLong(RxBuffer.NOT_STARTED);
    private final CompositeDisposable disposer = new CompositeDisposable();

    private Subject<T> output = PublishSubject.create();
    private LinkedBlockingQueue<T> buffer = new LinkedBlockingQueue<>();
    private int maxItems = 0;
    private Timeout timeout;

    private RxEmitter() {
        this.disposer.add(Disposable.fromRunnable(() -> {
            this.buffer.clear();
            this.timeout.stop();
            this.emitted.set(0);
            this.started.set(RxBuffer.NOT_STARTED);
        }));
    }

    public int size() {
        return this.buffer.size();
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super T> observer) {
        this.output.subscribe(observer);
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
        if (this.isTerminated()) {
            d.dispose();
        }
    }

    @Override
    public void onNext(@NonNull List<T> items) {
        this.start();

        this.buffer.addAll(items);

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
    public void accept(T item) {
        this.start();

        this.buffer.add(item);

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
            collectedItems.forEach(this.output::onNext);
        }
        this.started.set(RxBuffer.NOT_STARTED);
        this.emitted.set(Instant.now().toEpochMilli());
    }

    RxBuffer toRxBuffer() {
        var emitter = this;
        return new RxBuffer() {
            @Override
            public int size() {
                return emitter.size();
            }

            @Override
            public void emit() {
                emitter.emit();
            }

            @Override
            public long started() {
                return emitter.started.get();
            }

            @Override
            public long emitted() {
                return emitter.emitted.get();
            }

            @Override
            public boolean isDisposed() {
                return emitter.isTerminated();
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
        private RxEmitter<U> result = new RxEmitter<>();
        private Consumer<RxEmitter<U>> callback;
        public Builder<U> withMaxItems(int value) {
            this.result.maxItems = value;
            return this;
        }

        public Builder<U> withMaxTimeInMs(long value) {
            this.maxTimeInMs = value;
            return this;
        }

        Builder<U> withTimeout(Timeout timeout) {
            this.result.timeout = timeout;
            return this;
        }

        Builder<U> onBuilt(Consumer<RxEmitter<U>> callback) {
            this.callback = callback;
            return this;
        }

        public RxEmitter<U> build() {
            if (this.result.maxItems < 1 && this.maxTimeInMs < 1) {
                throw new IllegalStateException("Collector must be set to hold max items or max time");
            }
            if (0 < this.maxTimeInMs && this.result.timeout != null && this.result.timeout.getTimeoutInMs() != this.maxTimeInMs) {
                throw new InvalidConfigurationException("Mismatching timeouts! The timeout set for the collector is " + this.result.timeout.getTimeoutInMs() + " in ms and the builder has a timeout controller, which has " + this.maxTimeInMs + " timeout in ms");
            }
            if (this.maxTimeInMs < 1) {
                this.result.timeout = Timeout.createVoidTimeout();
            } else if (this.result.timeout == null) {
                this.result.timeout = new TimeoutController(this.maxTimeInMs).addRxEmitter(this.result);
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
