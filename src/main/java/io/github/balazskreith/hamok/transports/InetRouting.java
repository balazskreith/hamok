package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.common.MapUtils;
import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.rxutils.RxTimeLimitedMap;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class InetRouting {

    private RwLock rwLock;
    private Map<UUID, InetAddress> source;
    private AtomicReference<Map<UUID, InetAddress>> endpointToAddress;
    private AtomicReference<Map<InetAddress, UUID>> addressToEndpoint;

    InetRouting() {
        this.rwLock = new RwLock();
        this.source = new RxTimeLimitedMap(5000);
        this.endpointToAddress = new AtomicReference<>(Collections.EMPTY_MAP);
        this.addressToEndpoint = new AtomicReference<>(Collections.EMPTY_MAP);
    }

    public void add(UUID endpointId, InetAddress address) {
        this.rwLock.runInWriteLock(() -> {
            this.source.put(endpointId, address);
        });
        this.make();
    }

    public boolean hasAddress(InetAddress address) {
        if (address == null) return false;
        return this.addressToEndpoint.get().containsKey(address);
    }


    private boolean remove(UUID endpointId) {
        var modified = this.rwLock.supplyInWriteLock(() -> {
            return this.source.remove(endpointId) != null;
        });
        if (modified) {
            this.make();
        }
        return modified;
    }

    public InetAddress get(UUID endpointId) {
        if (endpointId == null) return null;
        return this.endpointToAddress.get().get(endpointId);
    }

    private void make() {
        this.rwLock.runInReadLock(() -> {
            var endpointToAddress = Map.copyOf(this.source);
            this.endpointToAddress.set(endpointToAddress);
            var addressToEndpoint = MapUtils.invertMap(endpointToAddress);
            this.addressToEndpoint.set(addressToEndpoint);
        });
    }
}
