package io.github.balazskreith.hamok.storagegrid.discovery;

import io.github.balazskreith.hamok.storagegrid.messages.EndpointStatesNotification;
import io.github.balazskreith.hamok.storagegrid.messages.HelloNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InactiveState extends AbstractState {
    private static final Logger logger = LoggerFactory.getLogger(InactiveState.class);

    InactiveState(Discovery base) {
        super(base);
    }

    @Override
    protected void acceptEndpointStateNotification(EndpointStatesNotification notification) {
        // no effect
    }

    @Override
    protected void acceptHelloNotification(HelloNotification notification) {
        // no effect
    }


    @Override
    States name() {
        return States.INACTIVE;
    }

    @Override
    public void run() {
        // void run
    }
}
