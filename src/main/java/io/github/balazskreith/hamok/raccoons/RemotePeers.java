package io.github.balazskreith.hamok.raccoons;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RemotePeers {

    private static final Logger logger = LoggerFactory.getLogger(RemotePeers.class);

    private final Subject<UUID> detachedRemotePeerIds = PublishSubject.create();
    private final Subject<UUID> joinedRemotePeerIds = PublishSubject.create();
    private final Map<UUID, RemotePeer> remotePeers = new ConcurrentHashMap<>();
    private final AtomicReference<Set<UUID>> activeRemotePeerIds = new AtomicReference<>(Collections.emptySet());
    private final Random random = new Random();
    private final AtomicInteger hashCode = new AtomicInteger(random.nextInt());
    void reset() {
        var activeRemoteIds = this.activeRemotePeerIds.get();
        activeRemoteIds.forEach(this::detach);
        this.updateRemotePeerIds();
    }

    void detach(UUID remotePeerId) {
        var remotePeer = this.remotePeers.remove(remotePeerId);
        if (remotePeer == null) {
            logger.warn("Cannot detach peer {}, because it has not been registered as remote peer", remotePeerId);
            return;
        }
        this.updateRemotePeerIds();
        this.detachedRemotePeerIds.onNext(remotePeerId);
    }

    RemotePeer get(UUID remotePeerId) {
        return this.remotePeers.get(remotePeerId);
    }

    void join(UUID remotePeerId) {
        var remotePeer = new RemotePeer(
                remotePeerId,
                Instant.now().toEpochMilli()
        );
        var prevRemotePeer = this.remotePeers.put(remotePeerId, remotePeer);
        if (prevRemotePeer != null) {
            return;
        }
        this.updateRemotePeerIds();
        this.joinedRemotePeerIds.onNext(remotePeerId);
    }

    void touch(UUID remotePeerId) {
        var remotePeer = this.remotePeers.get(remotePeerId);
        if (remotePeer == null) {
            this.join(remotePeerId);
            return;
        }
        this.remotePeers.put(remotePeerId, new RemotePeer(
                remotePeerId,
                Instant.now().toEpochMilli()
        ));
        this.hashCode.set(this.hashCode.get() + this.random.nextInt());
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
        return this.hashCode.get();
    }

    public int size() {
        return this.activeRemotePeerIds.get().size();
    }

    public Set<UUID> getRemotePeerIds() {
        return this.activeRemotePeerIds.get();
    }

    private void updateRemotePeerIds() {
        var activeRemotePeerIds = this.remotePeers.values().stream()
                .map(RemotePeer::id)
                .collect(Collectors.toSet());

        this.hashCode.set(this.hashCode.get() + this.random.nextInt());
        this.activeRemotePeerIds.set(activeRemotePeerIds);
    }
}
