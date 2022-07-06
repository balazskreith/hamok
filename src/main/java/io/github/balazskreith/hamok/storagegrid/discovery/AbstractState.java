package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.racoon.events.EndpointStatesNotification;
import io.github.balazskreith.hamok.racoon.events.HelloNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractState implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractState.class);

    protected final Discovery base;

    AbstractState(Discovery base) {
        this.base = base;
    }

    protected abstract void acceptEndpointStateNotification(EndpointStatesNotification notification);

    protected abstract void acceptHelloNotification(HelloNotification notification);

    abstract States name();
}
