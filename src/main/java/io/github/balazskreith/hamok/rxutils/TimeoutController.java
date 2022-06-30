package io.github.balazskreith.hamok.rxutils;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TimeoutController implements Timeout {
    private static long MIN_TIMEOUT_IN_MS = 10;

    private final long maxTimeInMs;
    private final Scheduler scheduler;
    private AtomicReference<Disposable> timer = new AtomicReference<>(null);
    private Map<UUID, RxBuffer> rxBuffers = new ConcurrentHashMap<>();
    private volatile boolean stopped = false;

    public TimeoutController(long timeoutInMs) {
        this(timeoutInMs, Schedulers.computation());
    }

    public TimeoutController(long timeoutInMs, Scheduler scheduler) {
        if (timeoutInMs < 1) {
            throw new IllegalStateException("timeout cannot be less than 1 ms for a timeout controller");
        }
        this.maxTimeInMs = timeoutInMs;
        this.scheduler = scheduler;
    }

    public<T> RxCollector.Builder<T> rxCollectorBuilder() {
        return RxCollector.<T>builder()
                .withTimeout(this)
                .onBuilt(this::addRxCollector)
                .withMaxTimeInMs(this.maxTimeInMs);
    }

    <T> TimeoutController addRxCollector(RxCollector<T> collector) {
        var id = UUID.randomUUID();
        var rxBuffer = collector.toRxBuffer();
        this.rxBuffers.put(id, rxBuffer);
        collector.doOnTerminate(() -> {
           this.rxBuffers.remove(id);
        });
        return this;
    }

    public TimeoutController addRunnable(Runnable runnable) {
        var id = UUID.randomUUID();
        var run = new AtomicBoolean(false);
        var started = Instant.now().toEpochMilli();
        var emitted = new AtomicLong(0);
        RxBuffer rxBuffer = new RxBuffer() {
            @Override
            public int size() {
                return run.get() ? 0 : 1;
            }

            @Override
            public void emit() {
                if (run.compareAndSet(false, true)) {
                    runnable.run();
                    emitted.set(Instant.now().toEpochMilli());
                    rxBuffers.remove(id);
                }
            }

            @Override
            public long started() {
                return started;
            }

            @Override
            public long emitted() {
                return emitted.get();
            }

            @Override
            public boolean isDisposed() {
                return run.get();
            }
        };
        this.rxBuffers.put(id, rxBuffer);
        return this;
    }

    public<T> RxEmitter.Builder<T> rxEmitterBuilder() {
        return RxEmitter.<T>builder()
                .withTimeout(this)
                .onBuilt(this::addRxEmitter)
                .withMaxTimeInMs(this.maxTimeInMs);
    }

    <T> TimeoutController addRxEmitter(RxEmitter<T> emitter) {
        var id = UUID.randomUUID();
        var rxBuffer = emitter.toRxBuffer();
        this.rxBuffers.put(id, rxBuffer);
        emitter.doOnTerminate(() -> {
            this.rxBuffers.remove(id);
        });
        return this;
    }

    @Override
    public long getTimeoutInMs() {
        return this.maxTimeInMs;
    }

    @Override
    public void start() {
        this.stopped = false;
        this.startTimer(this.maxTimeInMs);
    }

    @Override
    public void stop() {
        this.stopped = true;
    }

    private void startTimer(long timeoutInMs) {
        if (this.timer.get() != null || this.maxTimeInMs < 1) {
            return;
        }
//        var timeoutInMs = Math.max(10, Math.min(this.maxTimeInMs, Instant.now().toEpochMilli() - this.emitted.get()));
        var timer = this.scheduler.scheduleDirect(() -> {
            if (this.stopped) {
                this.timer.set(null);
                return;
            }
            var now = Instant.now().toEpochMilli();
            var it = this.rxBuffers.values().iterator();
            var reschedule = false;
            var nextTimeoutInMs = this.maxTimeInMs;
            for (; it.hasNext(); ) {
                var rxBuffer = it.next();
                if (rxBuffer.isDisposed()) {
                    it.remove();
                    continue;
                }
                var elapsedTimeInMs = now - rxBuffer.emitted();
                if (elapsedTimeInMs < this.maxTimeInMs) {
                    if (0 < rxBuffer.size()) {
                        reschedule = true;
                        nextTimeoutInMs = Math.min(nextTimeoutInMs, now - rxBuffer.started());
                    }
                    reschedule = reschedule || 0 < rxBuffer.size();
                    continue;
                }
                rxBuffer.emit();
            }
            this.timer.set(null);
            if (!this.stopped && reschedule) {
                nextTimeoutInMs = Math.max(nextTimeoutInMs, MIN_TIMEOUT_IN_MS);
                this.startTimer(nextTimeoutInMs);
            }
        }, timeoutInMs, TimeUnit.MILLISECONDS);
        if (this.timer.compareAndSet(null, timer) == false) {
            timer.dispose();
        }
    }
}
