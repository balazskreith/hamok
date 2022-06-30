package io.github.balazskreith.hamok.storagegrid.discovery;

import io.reactivex.rxjava3.core.Scheduler;

import java.util.Objects;
import java.util.UUID;

public class DiscoveryBuilder {
    private volatile boolean built = false;
    private UUID localEndpointId = null;
    private int maxIdleRemoteEndpointInMs = 5000;
    private int heartbeatInMs = 500;
    private int helloNotificationPeriodInMs = 1500;
    private int endpointNotificationPeriodInMs = 1500;

    private Discovery result = new Discovery();

    public DiscoveryBuilder withScheduler(Scheduler scheduler) {
        this.result.scheduler = scheduler;
        return this;
    }

    public DiscoveryBuilder withLocalEndpointId(UUID localEndpointId) {
        this.localEndpointId = localEndpointId;
        return this;
    }

    public DiscoveryBuilder withMaxIdleRemoteEndpointId(int maxIdleRemoteEndpointInMs) {
        this.maxIdleRemoteEndpointInMs = maxIdleRemoteEndpointInMs;
        return this;
    }

    public DiscoveryBuilder withHeartbeatInMs(int heartbeatInMs) {
        this.heartbeatInMs = heartbeatInMs;
        return this;
    }

    public DiscoveryBuilder withHelloNotificationPeriodInMs(int helloNotificationPeriodInMs) {
        this.helloNotificationPeriodInMs = helloNotificationPeriodInMs;
        return this;
    }

    public DiscoveryBuilder withEndpointNotificationPeriodInMs(int endpointNotificationPeriodInMs) {
        this.endpointNotificationPeriodInMs = endpointNotificationPeriodInMs;
        return this;
    }



    public Discovery build() {
        if (this.built) {
            throw new IllegalStateException("Cannot built a twice");
        }
        Objects.requireNonNull(this.localEndpointId, "Local endpoint cannot be null");
        try {
            this.result.config = new DiscoveryConfig(
                    this.localEndpointId,
                    this.maxIdleRemoteEndpointInMs,
                    heartbeatInMs,
                    helloNotificationPeriodInMs,
                    endpointNotificationPeriodInMs
            );
            return this.result;
        } finally {
            this.built = true;
        }
    }
}
