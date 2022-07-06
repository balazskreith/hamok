package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.racoon.events.EndpointStatesNotification;
import io.github.balazskreith.hamok.racoon.events.HelloNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

class PropagatorState extends AbstractState {
    private static final Logger logger = LoggerFactory.getLogger(ListenerState.class);

    PropagatorState(Discovery base) {
        super(base);
    }

    private Queue<UUID> reactivatedEndpoints = new LinkedBlockingQueue<>();
    private AtomicLong lastPropagated = new AtomicLong(0);

    @Override
    States name() {
        return States.PROPAGATOR;
    }

    @Override
    public void run() {
        var now = Instant.now().toEpochMilli();
        var elapsedInMs = now - this.lastPropagated.get();
        if (elapsedInMs < this.base.config.endpointStateNotificationPeriodInMs()) {
            return;
        }
        var detachedEndpointIds = new HashSet<UUID>();

        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        for (var it = activeRemoteEndpointIds.entrySet().iterator(); it.hasNext(); ) {
            var entry = it.next();
            var remoteEndpointId = entry.getKey();
            var lastUpdated = entry.getValue();
            if (this.base.getMaxIdleRemoteEndpointInMs() < now - lastUpdated) {
                detachedEndpointIds.add(remoteEndpointId);
            }
        }
        Set<UUID> remoteEndpointIds = this.base.getRemoteEndpointIds();
        Set<UUID> reactivatedEndpointIds;
        if (this.reactivatedEndpoints.isEmpty() == false) {
            reactivatedEndpointIds = new HashSet<>();
            while (this.reactivatedEndpoints.isEmpty() == false) {
                reactivatedEndpointIds.add(this.reactivatedEndpoints.poll());
            }
            remoteEndpointIds = SetUtils.addAll(remoteEndpointIds, reactivatedEndpointIds);
        } else {
            reactivatedEndpointIds = Collections.EMPTY_SET;
        }

        detachedEndpointIds.forEach(this.base::detachedRemoteEndpointId);
        this.base.sendEndpointStateNotifications(remoteEndpointIds);
        // at this point we informed all the reactivated endpoints with the info they have been inactive,
        // so we can safely join them again.
        reactivatedEndpointIds.forEach(this.base::joinRemoteEndpoint);
        this.lastPropagated.set(now);

    }

    @Override
    protected void acceptEndpointStateNotification(EndpointStatesNotification notification) {
        if (UuidTools.equals(this.base.getLocalEndpointId(), notification.sourceEndpointId())) {
            // loopback?
            return;
        }
        // there should be no two propagator in the cluster
        // so we are going to add the source as an endpoint and let the election process (raft)
        // figure out who is the true propagator
        var inactiveRemoteEndpointIds = this.base.getInactiveRemoteEndpointIds();
        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        var remoteEndpointId = notification.sourceEndpointId();
        if (inactiveRemoteEndpointIds.containsKey(notification.sourceEndpointId())) {
            // if it was inactivated by anyone, and we get a notification from it as propagator,
            // then that endpoint should not be the producer, and the election process
            // has to handle to have only one producer in one cluster
            // until that we are not accepting anything from that producer
            logger.warn("An inactive endpoint {} propagated an endpoints state message", remoteEndpointId);
            return;
        }
        if (activeRemoteEndpointIds.put(remoteEndpointId, Instant.now().toEpochMilli()) == null) {
            this.base.joinRemoteEndpoint(remoteEndpointId);
        }
    }

    @Override
    protected void acceptHelloNotification(HelloNotification notification) {
        var localEndpointId = this.base.getLocalEndpointId();
        var remoteEndpointId = notification.sourcePeerId();
        if (localEndpointId == remoteEndpointId) {
            // loopback?
            return;
        }
        var inactiveRemoteEndpointIds = this.base.getInactiveRemoteEndpointIds();
        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        var now = Instant.now().toEpochMilli();
        if (inactiveRemoteEndpointIds.containsKey(remoteEndpointId)) {
            // we must make sure that remote endpoint got the message that it was inactivated.
            this.reactivatedEndpoints.add(remoteEndpointId);
            return;
        }
        if (activeRemoteEndpointIds.put(remoteEndpointId, now) == null) {
            this.base.joinRemoteEndpoint(remoteEndpointId);
        }
    }
}
