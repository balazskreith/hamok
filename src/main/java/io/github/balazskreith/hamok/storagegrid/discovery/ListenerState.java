package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.EndpointStatesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.HelloNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

class ListenerState extends AbstractState {
    private static final Logger logger = LoggerFactory.getLogger(ListenerState.class);

    private final AtomicLong lastUpdated = new AtomicLong(-1);
    private final AtomicLong saidHello = new AtomicLong(-1);

    ListenerState(Discovery base) {
        super(base);
    }


    @Override
    States name() {
        return States.LISTENER;
    }

    @Override
    public void run() {
        var lastUpdated = this.lastUpdated.get();
        var now = Instant.now().toEpochMilli();
        var elapsedSinceHello = now - this.saidHello.get();
        if (this.base.config.helloNotificationPeriodInMs() < elapsedSinceHello) {
            return;
        }
        var elapsedSinceUpdatedInMs =  now - lastUpdated;
        if (0 < lastUpdated && this.base.getMaxIdleRemoteEndpointInMs() < elapsedSinceUpdatedInMs) {
            // at this point we have not been updated for a longer period of time then it should be,
            // but the responsibility to inactivate the discovery lies in the upper layer
            logger.warn("Discovery protocol have not been updated for {} milliseconds", elapsedSinceUpdatedInMs);
            return;
        }
        this.base.sendHelloNotification();
        this.saidHello.set(now);
    }

    @Override
    protected void acceptEndpointStateNotification(EndpointStatesNotification notification) {
        var resetRequest = false;
        var now = Instant.now().toEpochMilli();
        var localEndpointId = this.base.getLocalEndpointId();
        var inactiveRemoteEndpointIds = this.base.getInactiveRemoteEndpointIds();
        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        this.lastUpdated.set(now);
        if (notification.activeEndpointIds() != null) {
            var joinedRemoteEndpoints = notification.activeEndpointIds().stream()
                .filter(activeRemoteEndpointId -> UuidTools.notEquals(activeRemoteEndpointId, localEndpointId))
                .filter(activeRemoteEndpointId -> !activeRemoteEndpointIds.containsKey(activeRemoteEndpointId))
                .collect(Collectors.toSet());
            this.base.setJoinedRemoteEndpoints(joinedRemoteEndpoints);
        }

        if (notification.inactiveEndpointIds() != null) {
            // am I inactivated???
            resetRequest = notification.inactiveEndpointIds().stream().anyMatch(endpointId -> endpointId == localEndpointId);
            // someone else?
            var detachedRemoteEndpointIds = notification.inactiveEndpointIds().stream()
                .filter(inactiveRemoteEndpointId -> UuidTools.notEquals(inactiveRemoteEndpointId, localEndpointId))
                .filter(inactiveRemoteEndpointId -> !inactiveRemoteEndpointIds.containsKey(inactiveRemoteEndpointId))
                .collect(Collectors.toSet());
            this.base.setDetachedRemoteEndpointIds(detachedRemoteEndpointIds);
        }
        if (resetRequest) {
            this.base.resetLocalEndpoint();
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
            // only the leader can reactivate an inactivated endpoint
            return;
        }
        if (activeRemoteEndpointIds.put(remoteEndpointId, now) == null) {
            joinedRemoteEndpointIds.add(remoteEndpointId);
        }
        this.base.setJoinedRemoteEndpoints(joinedRemoteEndpointIds);
    }
}
