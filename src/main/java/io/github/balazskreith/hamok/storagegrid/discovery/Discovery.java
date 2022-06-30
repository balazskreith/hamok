package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.storagegrid.messages.EndpointStatesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.HelloNotification;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Discovery of remote endpoints
 *
 */
public class Discovery implements Disposable {
    private static final Logger logger = LoggerFactory.getLogger(Discovery.class);

    public static DiscoveryBuilder builder() {
        return new DiscoveryBuilder();
    }

    // set by the builder
    Scheduler scheduler = Schedulers.computation();
    DiscoveryConfig config;

    private AtomicReference<AbstractState> actual = new AtomicReference<>(new InactiveState(this));

    private final Subject<HelloNotification> outboundHelloNotifications = PublishSubject.<HelloNotification>create().toSerialized();
    private final Subject<EndpointStatesNotification> outboundEndpointNotification = PublishSubject.<EndpointStatesNotification>create().toSerialized();
    private final DiscoveryEvents events = new DiscoveryEvents();
    private final AtomicReference<Disposable> timer = new AtomicReference<>(null);

    private final Map<UUID, Long> activeRemoteEndpointIds =  new ConcurrentHashMap<>();
    private final Map<UUID, Long> inactiveRemoteEndpointIds =  new ConcurrentHashMap<>();

    private final AtomicReference<Set<UUID>> remoteEndpointIds = new AtomicReference<>(Collections.emptySet());

    private final Disposer disposer;


    Discovery() {
        this.disposer = Disposer.builder()
                .addSubject(this.outboundHelloNotifications)
                .addSubject(this.outboundEndpointNotification)
                .addDisposable(Disposable.fromRunnable(() -> {
                    var timer = this.timer.getAndSet(null);
                    if (timer != null && !timer.isDisposed()) {
                        timer.dispose();
                    }
                }))
                .build();
    }

    /**
     * Stop sending any messages and remove all discovered remote endpoints
     * Prevent being the lead, because it is already stopped
     */
    public void inactivate() {
        var state = this.actual.get();
        if (States.INACTIVE.equals(state.name())) {
            return;
        }
        if (!this.actual.compareAndSet(state, new InactiveState(this))) {
            logger.warn("Tried to change the state from {} to {}, but concurrently another state change has been performed", state.name(), States.INACTIVE.name());
            return;
        }
        this.stopTimer();
        logger.info("State changed from {} to {}", state.name(), States.INACTIVE.name());
    }


    public void propagate() {
        var state = this.actual.get();
        if (States.PROPAGATOR.equals(state.name())) {
            return;
        }
        if (!this.actual.compareAndSet(state, new PropagatorState(this))) {
            logger.warn("Tried to change the state from {} to {}, but concurrently another state change has been performed", state.name(), States.PROPAGATOR.name());
            return;
        }
        this.startTimer();
        logger.info("State changed from {} to {}", state.name(), States.PROPAGATOR.name());
    }

    public void listen() {
        var state = this.actual.get();
        if (States.LISTENER.equals(state.name())) {
            return;
        }
        if (!this.actual.compareAndSet(state, new ListenerState(this))) {
            logger.warn("Tried to change the state from {} to {}, but concurrently another state change has been performed", state.name(), States.LISTENER.name());
            return;
        }
        this.startTimer();
        logger.info("State changed from {} to {}", state.name(), States.LISTENER.name());
    }

    public Observable<HelloNotification> helloNotifications() {
        return this.outboundHelloNotifications;
    }

    public Observable<EndpointStatesNotification> endpointStatesNotifications() {
        return this.outboundEndpointNotification;
    }

    public Set<UUID> getRemoteEndpointIds() {
        return this.remoteEndpointIds.get();
    }

    public DiscoveryEvents events() {
        return this.events;
    }

    private void startTimer() {
        if (this.timer.get() != null) {
            return;
        }
        Disposable disposable = this.scheduler.createWorker().schedulePeriodically(
                () -> {
                    var state = this.actual.get();
                    Objects.requireNonNull(state);
                    state.run();
                },
                this.config.heartbeatInMs(),
                this.config.heartbeatInMs(),
                TimeUnit.MILLISECONDS
        );
        if (!this.timer.compareAndSet(null, disposable)) {
            disposable.dispose();
            logger.warn("Attempted to start the timer twice concurrently");
        }
    }

    private void stopTimer() {
        var timer = this.timer.getAndSet(null);
        if (timer != null && !timer.isDisposed()) {
            timer.dispose();
        }
    }

    UUID getLocalEndpointId() {
        return this.config.localEndpointId();
    }


    Map<UUID, Long> getInactiveRemoteEndpointIds() {
        return this.inactiveRemoteEndpointIds;
    }

    Map<UUID, Long> getActiveRemoteEndpointIds() {
        return this.activeRemoteEndpointIds;
    }


    void resetLocalEndpoint() {

    }

    void setJoinedRemoteEndpoints(Set<UUID> joinedRemoteEndpointIds) {
        if (joinedRemoteEndpointIds.size() < 1) {
            return;
        }
        var now = Instant.now().toEpochMilli();
        for (var it = joinedRemoteEndpointIds.iterator(); it.hasNext(); ) {
            var joinedRemoteEndpointId = it.next();
            this.inactiveRemoteEndpointIds.remove(joinedRemoteEndpointId);
            this.activeRemoteEndpointIds.put(joinedRemoteEndpointId, now);
        }
        var remoteEndpointIds = Set.copyOf(this.activeRemoteEndpointIds.keySet());
        this.remoteEndpointIds.set(remoteEndpointIds);
        joinedRemoteEndpointIds.forEach(this.events.remoteEndpointJoined::onNext);
    }

    void setDetachedRemoteEndpointIds(Set<UUID> detachedRemoteEndpointIds) {
        if (detachedRemoteEndpointIds.size() < 1) {
            return;
        }
        var now = Instant.now().toEpochMilli();
        for (var it = detachedRemoteEndpointIds.iterator(); it.hasNext(); ) {
            var detachedRemoteEndpointId = it.next();
            this.activeRemoteEndpointIds.remove(detachedRemoteEndpointId);
            this.inactiveRemoteEndpointIds.put(detachedRemoteEndpointId, now);
        }
        var remoteEndpointIds = Set.copyOf(this.activeRemoteEndpointIds.keySet());
        this.remoteEndpointIds.set(remoteEndpointIds);
        detachedRemoteEndpointIds.forEach(this.events.remoteEndpointDetached::onNext);
    }

    void sendEndpointStateNotifications(Set<UUID> remoteEndpointIds) {
        Set<UUID> activeEndpointIds;
        Set<UUID> inactiveEndpointIds;
        synchronized (this) {
            activeEndpointIds = Set.copyOf(this.activeRemoteEndpointIds.keySet());
            inactiveEndpointIds = Set.copyOf(this.inactiveRemoteEndpointIds.keySet());
        }
        var localEndpointId = this.getLocalEndpointId();
        for (var it = remoteEndpointIds.iterator(); it.hasNext(); ) {
            var remoteEndpointId = it.next();
            var notification = new EndpointStatesNotification(localEndpointId, activeEndpointIds, inactiveEndpointIds, remoteEndpointId);
            this.outboundEndpointNotification.onNext(notification);
        }
    }

    void sendHelloNotification() {
        var notification = new HelloNotification(this.config.localEndpointId());
        this.outboundHelloNotifications.onNext(notification);
    }

    int getMaxIdleRemoteEndpointInMs() {
        return this.config.maxIdleRemoteEndpointInMs();
    }


    @Override
    public void dispose() {
        if (!this.disposer.isDisposed()) {
            this.disposer.dispose();
        }
    }

    @Override
    public boolean isDisposed() {
        return this.disposer.isDisposed();
    }


    public void acceptHelloNotification(HelloNotification notification) {
        var state = this.actual.get();
        if (state != null) {
            state.acceptHelloNotification(notification);
        }
    }

    public void acceptEndpointStatesNotification(EndpointStatesNotification notification) {
        var state = this.actual.get();
        if (state != null) {
            state.acceptEndpointStateNotification(notification);
        }
    }
}
