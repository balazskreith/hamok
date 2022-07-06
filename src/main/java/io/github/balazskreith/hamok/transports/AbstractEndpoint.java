package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;


public abstract class AbstractEndpoint implements Endpoint {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEndpoint.class);

    private final AtomicReference<Thread> thread;
    private final Subject<Message> inbound;
    private final Subject<Message> outbound;
    private List<Observer<? super Message>> observers = Collections.synchronizedList(new LinkedList<>());
    private UUID endpointId = null;

    protected AbstractEndpoint() {
        this.thread = new AtomicReference<>();
        this.inbound = PublishSubject.create();
        this.outbound = PublishSubject.create();
        this.outbound.subscribe(message -> {
            this.accept(message);
        });
    }

    private boolean filtering(Message m) {
        if (this.endpointId == null) {
            return true;
        }
        if (m.destinationId == null) {
            return true;
        }
        if (UuidTools.equals(m.destinationId, this.endpointId)) {
            return true;
        }
//        logger.warn("Dropping message destination id {}, local endpoint id: {}", m.destinationId, this.endpointId);
        return false;
    }

    @Override
    public Observable<Message> inboundChannel() {
        return this.inbound;
    }

    @Override
    public Observer<Message> outboundChannel() {
        return this.outbound;
    }

    public void start() {
        if (this.thread.get() != null) {
            logger.warn("Attempted to start twice");
            return;
        }
        var thread = new Thread(this::run);
        if (this.thread.compareAndSet(null, thread)) {
            thread.start();
        }
    }

    @Override
    public boolean isRunning() {
        return false;
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
    }

    protected void setEndpointId(UUID endpointId) {
        this.endpointId = endpointId;
    }

    protected UUID getEndpointId() {
        return this.endpointId;
    }

    protected abstract void run();

    protected void dispatch(Message message) {
        this.inbound.onNext(message);
    }

    protected abstract void accept(Message message);

    public boolean started() {
        return this.thread.get() != null;
    }
}
