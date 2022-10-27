package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.common.Utils;
import io.github.balazskreith.hamok.common.UuidTools;
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

    private void receive(Models.Message message) {
        if (!this.enabled) {
            return;
        }
        var sourceId = Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId);
        this.rwLock.runInReadLock(() -> {
            var source = this.transports.get(sourceId);
            if (source == null) {
                logger.warn("Cannot find source {} in router", sourceId);
            } else if (source.enabled == false) {
                logger.trace("Blocked message from {} type: {}, protocol: {} to {}, because the source is disabled", source.endpointId,
                        Utils.supplyIfTrue(message.hasType(), message::getType),
                        Utils.supplyIfTrue(message.hasProtocol(), message::getProtocol),
                        Utils.supplyIfTrue(message.hasDestinationId(), message::getDestinationId)
                );
                return;
            }
            var notRouted = true;
            for (var it = this.transports.values().iterator(); it.hasNext(); ) {
                var transport = it.next();
                if (UuidTools.equals(UUID.fromString(message.getSourceId()), transport.endpointId)) {
                    continue;
                }
                if (!transport.enabled) {
                    logger.trace("Blocked message from {} to (transport id: {}, message destination: {}), type: {}, protocol: {}, because the destination is disabled",
                            source.endpointId,
                            transport.endpointId,
                            Utils.supplyIfTrue(message.hasDestinationId(), message::getDestinationId),
                            Utils.supplyIfTrue(message.hasType(), message::getType),
                            Utils.supplyIfTrue(message.hasProtocol(), message::getProtocol)
                    );
                    continue;
                }
                var destinationId = Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId);
                if (destinationId == null || UuidTools.equals(destinationId, transport.endpointId)) {
                    transport.transport.getReceiver().onNext(message);
                    notRouted = false;
                }
            }
            if (notRouted) {
                logger.warn("Message is NOT routed from {} type: {}, protocol {}",
                        source.endpointId,
                        Utils.supplyIfTrue(message.hasType(), message::getType),
                        Utils.supplyIfTrue(message.hasProtocol(), message::getProtocol)
                );
            }

        });
    }

    private record Transport(UUID endpointId, StorageGridTransport transport, boolean enabled) {

    }
}
