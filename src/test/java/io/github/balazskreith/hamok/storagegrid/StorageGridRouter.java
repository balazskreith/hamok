package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class StorageGridRouter {
    private static final Logger logger = LoggerFactory.getLogger(StorageGridRouter.class);

    private final Map<UUID, Transport> transports = new HashMap<>();
    private final RwLock rwLock = new RwLock();
    private volatile boolean enabled = true;

    public StorageGridRouter() {

    }

    public void add(UUID endpointId, StorageGridTransport transport) {
        var item = new Transport(
                endpointId,
                transport,
                true
        );
        this.rwLock.runInWriteLock(() -> {
            this.transports.put(endpointId, item);
            transport.getSender().subscribe(this::receive);
            logger.info("Added {} to the router", endpointId);
        });
    }

    public void enable() {
        this.enabled = true;
    }

    public void enable(UUID id) {
        this.rwLock.runInWriteLock(() -> {
            logger.info("Enabling {}", id);
            var transport = this.transports.get(id);
            if (transport != null) {
                this.transports.put(id, new Transport(
                        id,
                        transport.transport,
                        true
                ));
            } else {
                logger.warn("Transport with id {} does not exists", id);
            }
        });
    }

    public void disable() {
        this.enabled = false;
    }

    public void disable(UUID id) {
        this.rwLock.runInWriteLock(() -> {
            logger.info("Disabling {}", id);
            var transport = this.transports.get(id);
            if (transport != null) {
                this.transports.put(transport.endpointId, new Transport(
                        id,
                        transport.transport,
                        false
                ));
            } else {
                logger.warn("Transport with id {} does not exists", id);
            }
        });
    }

    public boolean isDisabled(UUID id) {
        return this.rwLock.supplyInReadLock(() -> {
            var transport = this.transports.get(id);
            if (transport == null) return false;
            return transport.enabled == false;
        });
    }

    private void receive(Message message) {
        if (!this.enabled) {
            return;
        }
        this.rwLock.runInReadLock(() -> {
            var source = this.transports.get(message.sourceId);
            if (source == null) {
                logger.warn("Cannot find source {} in router", source.endpointId);
            } else if (source.enabled == false) {
                logger.trace("Blocked message from {} type: {}, protocol: {} to {}, because the source is disabled", source.endpointId, message.type, message.protocol, message.destinationId);
                return;
            }
            var notRouted = true;
            for (var it = this.transports.values().iterator(); it.hasNext(); ) {
                var transport = it.next();
                if (UuidTools.equals(message.sourceId, transport.endpointId)) {
                    continue;
                }
                if (!transport.enabled) {
                    logger.trace("Blocked message from {} to (transport id: {}, message destination: {}), type: {}, protocol: {}, because the destination is disabled", source.endpointId, transport.endpointId, message.destinationId,  message.type, message.protocol);
                    continue;
                }
                if (message.destinationId == null || UuidTools.equals(message.destinationId, transport.endpointId)) {
                    transport.transport.getReceiver().onNext(message);
                    notRouted = false;
                }
            }
            if (notRouted) {
                logger.warn("Message is NOT routed from {} type: {}, protocol {}", source.endpointId, message.type, message.protocol);
            }

        });
    }

    private record Transport(UUID endpointId, StorageGridTransport transport, boolean enabled) {

    }
}
