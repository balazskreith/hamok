package io.github.balazskreith.hamok.racoon;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemotePeers {

    private static final Logger logger = LoggerFactory.getLogger(RemotePeers.class);

    private final Subject<UUID> detachedRemotePeerIds = PublishSubject.create();
    private final Subject<UUID> joinedRemotePeerIds = PublishSubject.create();
    private final Map<UUID, RemotePeer> remotePeers = new ConcurrentHashMap<>();
    private final AtomicReference<Set<UUID>> activeRemotePeerIds = new AtomicReference<>(Collections.emptySet());
    private final AtomicReference<Set<UUID>> inactiveRemotePeerIds = new AtomicReference<>(Collections.emptySet());

    void reset() {
        this.remotePeers.clear();
        this.updateRemotePeerIds();
    }

    void detach(UUID remotePeerId) {
        var remotePeer = this.remotePeers.get(remotePeerId);
        if (remotePeer == null) {
            logger.warn("Cannot detach peer {}, because it has not been registered as remote peer", remotePeerId);
            return;
        }
        if (!remotePeer.active()) {
            logger.warn("Attempted to inactivate remote peer twice");
            return;
        }
        var updatedRemotePeer = new RemotePeer(
                remotePeerId,
                false,
                Instant.now().toEpochMilli()
        );
        this.remotePeers.put(remotePeerId, updatedRemotePeer);
        this.updateRemotePeerIds();
        this.detachedRemotePeerIds.onNext(remotePeerId);
    }

    RemotePeer get(UUID remotePeerId) {
        return this.remotePeers.get(remotePeerId);
    }

    void join(UUID remotePeerId) {
        var remotePeer = new RemotePeer(
                remotePeerId,
                true,
                Instant.now().toEpochMilli()
        );
        var prevRemotePeer = this.remotePeers.put(remotePeerId, remotePeer);
        if (prevRemotePeer != null && prevRemotePeer.active()) {
            return;
        }

        this.updateRemotePeerIds();
        this.joinedRemotePeerIds.onNext(remotePeerId);
    }

    void touch(UUID remotePeerId) {
        var remotePeer = this.remotePeers.get(remotePeerId);
        if (remotePeer == null || remotePeer.active() == false) {
            this.join(remotePeerId);
            return;
        }
        this.remotePeers.put(remotePeerId, new RemotePeer(
                remotePeerId,
                true,
                Instant.now().toEpochMilli()
        ));
    }

    public Observable<UUID> detachedRemoteEndpointIds() {
        return this.detachedRemotePeerIds;
    }

    public Observable<UUID> joinedRemoteEndpointIds() {
        return this.joinedRemotePeerIds;
    }

    public Iterator<RemotePeer> iterator() {
       return this.remotePeers.values().iterator();
    }

    public Stream<RemotePeer> stream() {
        return this.remotePeers.values().stream();
    }

    @Override
    public int hashCode() {
        var hashCode = this.remotePeers.values().stream()
                .map(r -> (r.id().hashCode() * (r.active() ? 31 : 63)) ^ ((r.touched() != null) ? r.touched().hashCode() : 0))
                .reduce((p,a) -> (p * a));
        if (hashCode.isEmpty()) return 0;
        return hashCode.get().intValue();

    }

    public int size() {
        return this.activeRemotePeerIds.get().size();
    }

    public Set<UUID> getActiveRemotePeerIds() {
        return this.activeRemotePeerIds.get();
    }

    public Set<UUID> getInActiveRemotePeerIds() {
        return this.inactiveRemotePeerIds.get();
    }

    private void updateRemotePeerIds() {
        var activeRemotePeerIds = this.remotePeers.values().stream()
                .filter(RemotePeer::active)
                .map(RemotePeer::id)
                .collect(Collectors.toSet());

        var inactiveRemotePeerIds = this.remotePeers.values().stream()
                .filter(r -> r.active() == false)
                .map(RemotePeer::id)
                .collect(Collectors.toSet());

        this.activeRemotePeerIds.set(activeRemotePeerIds);
        this.inactiveRemotePeerIds.set(inactiveRemotePeerIds);
    }
}
