package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.racoon.events.EndpointStatesNotification;
import io.github.balazskreith.hamok.racoon.events.HelloNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

class ListenerState extends AbstractState {
    private static final Logger logger = LoggerFactory.getLogger(ListenerState.class);

    private final AtomicLong lastUpdated = new AtomicLong(-1);
    private final AtomicLong saidHello = new AtomicLong(-1);
    private boolean warnedFlag = false;
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
        if (elapsedSinceHello < this.base.config.helloNotificationPeriodInMs()) {
            return;
        }
        var elapsedSinceUpdatedInMs =  now - lastUpdated;
        if (0 < lastUpdated && this.base.getMaxIdleRemoteEndpointInMs() < elapsedSinceUpdatedInMs) {
            // at this point we have not been updated for a longer period of time then it should be,
            // but the responsibility to inactivate the discovery lies in the upper layer
            if (!this.warnedFlag) {
                logger.warn("Discovery protocol have not been updated for {} milliseconds", elapsedSinceUpdatedInMs);
                this.warnedFlag = true;
            }
        }
        this.base.sendHelloNotification();
        this.saidHello.set(now);
        this.warnedFlag = false;
    }

    @Override
    protected void acceptEndpointStateNotification(EndpointStatesNotification notification) {
        var resetRequest = false;
        var now = Instant.now().toEpochMilli();
        var localEndpointId = this.base.getLocalEndpointId();
        var inactiveRemoteEndpointIds = this.base.getInactiveRemoteEndpointIds();
        var activeRemoteEndpointIds = this.base.getActiveRemoteEndpointIds();
        this.lastUpdated.set(now);
        logger.info("{} received endpoint state notifications {}", this.base.getLocalEndpointId(), notification);
        if (notification.activeEndpointIds() != null) {
            notification.activeEndpointIds().stream()
                .filter(activeRemoteEndpointId -> UuidTools.notEquals(activeRemoteEndpointId, localEndpointId))
                .filter(activeRemoteEndpointId -> !activeRemoteEndpointIds.containsKey(activeRemoteEndpointId))
                .forEach(this.base::joinRemoteEndpoint);
        }

        if (notification.inactiveEndpointIds() != null) {
            // am I inactivated???
            resetRequest = notification.inactiveEndpointIds().stream().anyMatch(endpointId -> UuidTools.equals(endpointId, localEndpointId));
            // someone else?
            notification.inactiveEndpointIds().stream()
                .filter(inactiveRemoteEndpointId -> UuidTools.notEquals(inactiveRemoteEndpointId, localEndpointId))
                .filter(inactiveRemoteEndpointId -> !inactiveRemoteEndpointIds.containsKey(inactiveRemoteEndpointId))
                    .forEach(this.base::detachedRemoteEndpointId);
        }
        if (resetRequest) {
            this.base.resetLocalEndpoint();
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
            // only the leader can reactivate an inactivated endpoint
            return;
        }
        if (activeRemoteEndpointIds.put(remoteEndpointId, now) == null) {
            this.base.joinRemoteEndpoint(remoteEndpointId);
        }
        if (notification.raftLeaderId() != null) {
            if (!activeRemoteEndpointIds.containsKey(notification.raftLeaderId())) {

            }
        }
    }
}
