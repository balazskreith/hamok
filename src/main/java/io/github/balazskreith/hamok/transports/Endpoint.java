package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;


public abstract class Endpoint extends Observable<Message> implements Observer<Message> {

    private static final Logger logger = LoggerFactory.getLogger(MulticastEndpoint.class);

    private final AtomicReference<Thread> thread;
    private AtomicReference<Subject<Message>> inbound;
    private AtomicReference<Subject<Message>> outbound;
    private List<Observer<? super Message>> observers = Collections.synchronizedList(new LinkedList<>());
    private UUID endpointId = null;

    protected Endpoint() {
        this.thread = new AtomicReference<>();
        this.inbound = new AtomicReference<>(PublishSubject.create());
        this.outbound = new AtomicReference<>(PublishSubject.create());
    }

    private boolean filtering(Message m) {
        if (this.endpointId == null) return true;
        if (m.destinationId == null) return true;
        if (UuidTools.equals(m.destinationId, this.endpointId)) return true;
        return false;
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super Message> observer) {
        this.observers.add(observer);
        this.inbound.get().filter(this::filtering).subscribe(observer);
    }

    @Override
    public void onSubscribe(@NonNull Disposable d) {
        this.outbound.get().onSubscribe(d);
    }

    @Override
    public void onNext(@NonNull Message message) {
        this.outbound.get().onNext(message);
    }

    @Override
    public void onError(@NonNull Throwable e) {
        this.outbound.get().onError(e);
    }

    @Override
    public void onComplete() {
        this.outbound.get().onComplete();
    }

    public void start() {
        if (this.thread.get() != null) {
            logger.warn("Attempted to start twice");
            return;
        }
        var thread = new Thread(this::process);
        if (this.thread.compareAndSet(null, thread)) {
            thread.start();
        }
    }

    public void stop() {
        var thread = this.thread.get();
        if (thread == null) {
            return;
        }
        try {
            thread.join(10000);
        } catch (InterruptedException e) {
            return;
        }
        if (!thread.isInterrupted() && thread.isAlive()) {
            thread.interrupt();
        }
        this.thread.set(null);
        var oldInbound = this.inbound.get();
        var oldOutbound = this.outbound.get();
        var newInbound = PublishSubject.<Message>create();
        var newOutbound = PublishSubject.<Message>create();
        if (this.inbound.compareAndSet(oldInbound, newInbound)) {
            this.observers.forEach(o -> newInbound.filter(this::filtering).subscribe(o));
        }
    }

    protected void setEndpointId(UUID endpointId) {
        this.endpointId = endpointId;
    }

    protected abstract void process();

    protected Observable<Message> outbound() {
        return this.outbound.get();
    }

    protected Observer<Message> inbound() {
        return this.inbound.get();
    }

    public boolean started() {
        return this.thread.get() != null;
    }
}
