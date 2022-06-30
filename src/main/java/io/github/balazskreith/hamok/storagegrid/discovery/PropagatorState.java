package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.EndpointStatesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.HelloNotification;
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
        if (this.base.config.endpointStateNotificationPeriodInMs() < elapsedInMs) {
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

        this.base.setDetachedRemoteEndpointIds(detachedEndpointIds);
        this.base.sendEndpointStateNotifications(remoteEndpointIds);
        // at this point we informed all the reactivated endpoints with the info they have been inactive,
        // so we can safely join them again.
        this.base.setJoinedRemoteEndpoints(reactivatedEndpointIds);
        this.lastPropagated.set(now);

    }

    @Override
    protected void acceptEndpointStateNotification(EndpointStatesNotification notification) {
        if (UuidTools.equals(this.base.getLocalEndpointId(), notification.sourceEndpointId())) {
            // loopback?
            return;
        }
        // there should be no two producers in the cluster
        // so we are going to add the source as an endpoint and let the election process (raft)
        // figure out who is the real leader
        var inactiveRemoteEndpointIds = this.base.getInactiveRemoteEndpointIds();
        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        if (inactiveRemoteEndpointIds.containsKey(notification.sourceEndpointId())) {
            // if it was inactivated by anyone, and we get a notification from it as producer,
            // then that endpoint should not be the producer, and the election process
            // has to handle to have only one producer in one cluster
            // until that we are not accepting anything from that producer
            return;
        }
        var remoteEndpointId = notification.sourceEndpointId();
        if (activeRemoteEndpointIds.put(remoteEndpointId, Instant.now().toEpochMilli()) == null) {
            this.base.setJoinedRemoteEndpoints(Set.of(remoteEndpointId));
        }
    }

    @Override
    protected void acceptHelloNotification(HelloNotification notification) {
        var localEndpointId = this.base.getLocalEndpointId();
        var remoteEndpointId = notification.sourceEndpointId();
        if (localEndpointId == remoteEndpointId) {
            // loopback?
            return;
        }
        var inactiveRemoteEndpointIds = this.base.getInactiveRemoteEndpointIds();
        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        var joinedRemoteEndpointIds = new HashSet<UUID>();
        var now = Instant.now().toEpochMilli();
        if (inactiveRemoteEndpointIds.containsKey(remoteEndpointId)) {
            // we must make sure that remote endpoint got the message that it was inactivated.
            this.reactivatedEndpoints.add(remoteEndpointId);
            return;
        }
        if (activeRemoteEndpointIds.put(remoteEndpointId, now) == null) {
            joinedRemoteEndpointIds.add(remoteEndpointId);
        }
        this.base.setJoinedRemoteEndpoints(joinedRemoteEndpointIds);
    }
}
