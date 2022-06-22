package com.balazskreith.vstorage.storagegrid.discovery;

import com.balazskreith.vstorage.common.Disposer;
import com.balazskreith.vstorage.storagegrid.messages.EndpointStatesNotification;
import com.balazskreith.vstorage.storagegrid.messages.HelloNotification;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Discovery of remote endpoints
 *
 * When discovery start it sends hello notification to a null destination endpoint, which means broadcasting
 * first phase of discovery ends when first endpoint state notification has received.
 * if the incoming endpoint state notification does not contain an endpoint we know about,
 * then this discovery sends an endpoint notification to the leader directly which accept and merge the information.
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

    private final Subject<HelloNotification> outboundHelloNotifications = PublishSubject.<HelloNotification>create().toSerialized();
    private final Subject<EndpointStatesNotification> outboundEndpointNotification = PublishSubject.<EndpointStatesNotification>create().toSerialized();
    private final DiscoveryEvents events = new DiscoveryEvents();
    private final AtomicReference<Disposable> helloTimer = new AtomicReference<>(null);
    private final AtomicReference<Disposable> stateTimer = new AtomicReference<>(null);

    private final Map<UUID, Long> activeRemoteEndpointIds =  new HashMap<>();
    private final Map<UUID, Long> inactiveRemoteEndpointIds =  new HashMap<>();

    private final AtomicReference<Set<UUID>> remoteEndpointIds = new AtomicReference<>(Collections.emptySet());

    private final Disposer disposer;

    Discovery() {
        this.disposer = Disposer.builder()
                .addSubject(this.outboundHelloNotifications)
                .addSubject(this.outboundEndpointNotification)
                .addDisposable(Disposable.fromRunnable(() -> {
                    var helloTimer = this.helloTimer.compareAndExchange(null, null);
                    if (helloTimer != null && !helloTimer.isDisposed()) {
                        helloTimer.dispose();
                    }
                    var stateTimer = this.stateTimer.compareAndExchange(null, null);
                    if (stateTimer != null && !stateTimer.isDisposed()) {
                        stateTimer.dispose();
                    }
//                    var stateTimer = this
                }))
                .build();
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

    public void acceptHelloNotification(HelloNotification notification) {
        var localEndpointId = this.config.localEndpointId();
        var remoteEndpointId = notification.sourceEndpointId();
        if (localEndpointId == remoteEndpointId) {
            // loopback?
            return;
        }
        var joinedRemoteEndpointIds = new HashSet<UUID>();
        var now = Instant.now().toEpochMilli();
        synchronized (this) {
            this.inactiveRemoteEndpointIds.remove(remoteEndpointId);
            if (this.activeRemoteEndpointIds.put(remoteEndpointId, now) == null) {
                joinedRemoteEndpointIds.add(remoteEndpointId);
            }
            if (0 < joinedRemoteEndpointIds.size()) {
                var updatedRemoteEndpointIds = Set.copyOf(this.activeRemoteEndpointIds.keySet());
                this.remoteEndpointIds.set(updatedRemoteEndpointIds);
            }
        }
        if (0 < joinedRemoteEndpointIds.size()) {
            joinedRemoteEndpointIds.forEach(this.events.remoteEndpointJoined::onNext);
        }
    }

    public void acceptEndpointStatesNotification(EndpointStatesNotification notification) {
        // only the leader should issue endpoint notification state
        var resetRequest = false;
        var joinedRemoteEndpointIds = new HashSet<UUID>();
        var detachedRemoteEndpointIds = new HashSet<UUID>();
        var now = Instant.now().toEpochMilli();
        var localEndpointId = this.config.localEndpointId();
        synchronized (this) {
            if (notification.activeEndpointIds() != null) {
                notification.activeEndpointIds().stream()
                        .filter(activeRemoteEndpointId -> activeRemoteEndpointId != localEndpointId)
                        .forEach(activeRemoteEndpointId -> {
                            if (this.activeRemoteEndpointIds.put(activeRemoteEndpointId, now) == null) {
                                joinedRemoteEndpointIds.add(activeRemoteEndpointId);
                            }
                        });
            }

            if (notification.inactiveEndpointIds() != null) {
                // am I inactivated???
                resetRequest = notification.inactiveEndpointIds().stream().anyMatch(endpointId -> endpointId == localEndpointId);
                // someone else?
                notification.inactiveEndpointIds().stream()
                        .filter(inactiveRemoteEndpointId -> inactiveRemoteEndpointId != localEndpointId)
                        .filter(inactiveRemoteEndpointId -> !this.inactiveRemoteEndpointIds.containsKey(inactiveRemoteEndpointId))
                        .forEach(inactiveEndpointId -> {
                            detachedRemoteEndpointIds.add(inactiveEndpointId);
                            this.inactiveRemoteEndpointIds.put(inactiveEndpointId, now);
                        });
            }
            if (0 < joinedRemoteEndpointIds.size() || 0 < detachedRemoteEndpointIds.size()) {
                var updatedRemoteEndpointIds = Set.copyOf(this.activeRemoteEndpointIds.keySet());
                this.remoteEndpointIds.set(updatedRemoteEndpointIds);
            }
        }
        if (0 < joinedRemoteEndpointIds.size()) {
            joinedRemoteEndpointIds.forEach(this.events.remoteEndpointJoined::onNext);
        }
        if (0 < detachedRemoteEndpointIds.size()) {
            detachedRemoteEndpointIds.forEach(this.events.remoteEndpointDetached::onNext);
        }
        if (resetRequest) {
            this.events.localEndpointInactivated.onNext(null);
        }
    }


    public boolean isSendingEndpointStates() {
        return this.stateTimer.get() != null;
    }

    public void startSendingEndpointStates() {
        if (this.stateTimer.get() != null) {
            logger.warn("Attempted to start twice");
            return;
        }
        synchronized (this) {
            if (this.stateTimer.get() != null) {
                return;
            }
            this.helloTimer.set(this.scheduler.createWorker().schedulePeriodically(
                    this::sendEndpointStates,
                    0,
                    1000,
                    TimeUnit.MILLISECONDS
            ));
        }
    }

    public void stopSendingEndpointStates() {
        if (this.stateTimer.get() == null) {
            return;
        }
        synchronized (this) {
            var timer = this.stateTimer.get();
            if (timer != null && !timer.isDisposed()) {
                timer.dispose();
            }
        }
    }

    public boolean isSendingHelloNotifications() {
        return this.helloTimer.get() != null;
    }

    public void startSendingHelloNotifications() {
        if (this.helloTimer.get() != null) {
            logger.warn("Attempted to start twice");
            return;
        }
        synchronized (this) {
            if (this.helloTimer.get() != null) {
                return;
            }
            Runnable sendHello = () -> {
                var notification = new HelloNotification(config.localEndpointId());
                this.outboundHelloNotifications.onNext(notification);
            };
            this.helloTimer.set(this.scheduler.createWorker().schedulePeriodically(
                    sendHello,
                    0,
                    1000,
                    TimeUnit.MILLISECONDS
            ));
        }
    }

    public void stopSendingHelloNotifications() {
        if (this.helloTimer.get() == null) {
            return;
        }
        synchronized (this) {
            var timer = this.helloTimer.get();
            if (timer != null && !timer.isDisposed()) {
                timer.dispose();
            }
        }
    }


    private void sendEndpointStates() {
        var now = Instant.now().toEpochMilli();
        var activeEndpointIds = new HashSet<UUID>();
        var inActiveEndpointIds = new HashSet<UUID>();
        var localEndpointId = this.config.localEndpointId();
        synchronized (this) {
            for (var it = this.activeRemoteEndpointIds.entrySet().iterator(); it.hasNext(); ) {
                var entry = it.next();
                var remoteEndpointId = entry.getKey();
                var lastUpdated = entry.getValue();
                if (config.maxIdleRemoteEndpointInMs() < now - lastUpdated) {
                    inActiveEndpointIds.add(remoteEndpointId);
                } else {
                    activeEndpointIds.add(remoteEndpointId);
                }
            }
            if (0 < inActiveEndpointIds.size()) {
                inActiveEndpointIds.forEach(this.activeRemoteEndpointIds::remove);
                var updatedRemoteEndpointIds = Set.copyOf(this.activeRemoteEndpointIds.keySet());
                this.remoteEndpointIds.set(updatedRemoteEndpointIds);
            }
            activeEndpointIds.add(localEndpointId);
            this.inactiveRemoteEndpointIds.keySet().forEach(inActiveEndpointIds::add);
        }

        activeEndpointIds.stream()
                .filter(endpointId -> endpointId != localEndpointId)
                .map(remoteEndpointId -> new EndpointStatesNotification(
                        activeEndpointIds,
                        inActiveEndpointIds,
                        remoteEndpointId
                )).forEach(notification -> {
                    this.outboundEndpointNotification.onNext(notification);
                });
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
}
