package io.github.balazskreith.hamok.raccoons;


import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.events.Events;
import io.github.balazskreith.hamok.raccoons.events.InboundEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

class RaccoonRouter {
    private static final Logger logger = LoggerFactory.getLogger(RaccoonRouter.class);

    private final Map<UUID, Peer> peers = new ConcurrentHashMap<>();
    private final Events receivedEvents = new Events();
    private volatile boolean enabled = true;

    public RaccoonRouter() {
        this.receivedEvents.endpointStateNotifications().subscribe(notification -> {
            var sourcePeer = this.peers.get(notification.sourceEndpointId());
            if (sourcePeer == null) {
                logger.warn("No source peer for notification {}", notification);
                return;
            }
            if (!sourcePeer.enabled()) {
                logger.info("Notification from {} is blocked, because the source is disabled", notification.sourceEndpointId());
                return;
            }
            var targetPeerId = notification.destinationEndpointId();
            for (var peer : this.peers.values()) {
                if (UuidTools.equals(peer.peerId(), sourcePeer.peerId())) {
                    continue;
                }
                if (targetPeerId != null && UuidTools.notEquals(targetPeerId, peer.peerId())) {
                    continue;
                }
                if (!peer.enabled()) {
                    logger.info("endpointStateNotifications from {} to {} is blocked, because the target is disabled", sourcePeer.peerId(), targetPeerId);
                }
                logger.info("{} sending endpointStateNotifications to {}. {}", sourcePeer.peerId(), targetPeerId, notification);
                peer.racoon.inboundEvents().endpointStateNotifications().onNext(notification);
            }
        });
        this.receivedEvents.appendEntriesRequest().subscribe(request -> {
            var sourcePeer = this.peers.get(request.leaderId());
            if (sourcePeer == null) {
                logger.warn("No source peer for request {}", request);
                return;
            }
            if (!sourcePeer.enabled()) {
                logger.info("Request from {} is blocked, because the source is disabled", sourcePeer.peerId());
                return;
            }
            var targetPeerId = request.peerId();
            for (var peer : this.peers.values()) {
                if (UuidTools.equals(peer.peerId(), sourcePeer.peerId())) {
                    continue;
                }
                if (targetPeerId != null && UuidTools.notEquals(targetPeerId, peer.peerId())) {
                    continue;
                }
                if (!peer.enabled()) {
                    logger.info("appendEntriesRequest from {} to {} is blocked, because the target is disabled", sourcePeer.peerId(), targetPeerId);
                }
                logger.info("{} sending appendEntriesRequest to {}. {}", sourcePeer.peerId(), targetPeerId, request);
                peer.racoon.inboundEvents().appendEntriesRequests().onNext(request);
            }
        });
        this.receivedEvents.appendEntriesResponse().subscribe(response -> {
            var sourcePeer = this.peers.get(response.sourcePeerId());
            if (sourcePeer == null) {
                logger.warn("No source peer for response {}", response);
                return;
            }
            if (!sourcePeer.enabled()) {
                logger.info("Response from {} is blocked, because the source is disabled", sourcePeer.peerId());
                return;
            }
            var targetPeerId = response.destinationPeerId();
            for (var peer : this.peers.values()) {
                if (UuidTools.equals(peer.peerId(), sourcePeer.peerId())) {
                    continue;
                }
                if (targetPeerId != null && UuidTools.notEquals(targetPeerId, peer.peerId())) {
                    continue;
                }
                if (!peer.enabled()) {
                    logger.info("appendEntriesResponse from {} to {} is blocked, because the target is disabled", sourcePeer.peerId(), targetPeerId);
                }
                logger.info("{} sending appendEntriesResponse to {}. {}", sourcePeer.peerId(), targetPeerId, response);
                peer.racoon.inboundEvents().appendEntriesResponses().onNext(response);
            }
        });
        this.receivedEvents.helloNotifications().subscribe(notification -> {
            var sourcePeer = this.peers.get(notification.sourcePeerId());
            if (sourcePeer == null) {
                logger.warn("No source peer for notification {}", notification);
                return;
            }
            if (!sourcePeer.enabled()) {
                logger.info("notification from {} is blocked, because the source is disabled", sourcePeer.peerId());
                return;
            }
            for (var peer : this.peers.values()) {
                if (UuidTools.equals(peer.peerId(), sourcePeer.peerId())) {
                    continue;
                }
                if (!peer.enabled()) {
                    logger.info("helloNotifications from {} to {} is blocked, because the target is disabled", sourcePeer.peerId(), peer.peerId());
                }
                logger.info("{} sending helloNotifications to {}. {}", sourcePeer.peerId(), peer.peerId(), notification);
                peer.racoon.inboundEvents().helloNotifications().onNext(notification);
            }
        });
        this.receivedEvents.voteRequests().subscribe(request -> {
            var sourcePeer = this.peers.get(request.candidateId());
            if (sourcePeer == null) {
                logger.warn("No source peer for request {}", request);
                return;
            }
            if (!sourcePeer.enabled()) {
                logger.info("Request from {} is blocked, because the source is disabled", sourcePeer.peerId());
                return;
            }
            var targetPeerId = request.peerId();
            for (var peer : this.peers.values()) {
                if (UuidTools.equals(peer.peerId(), sourcePeer.peerId())) {
                    continue;
                }
                if (targetPeerId != null && UuidTools.notEquals(targetPeerId, peer.peerId())) {
                    continue;
                }
                if (!peer.enabled()) {
                    logger.info("voteRequests from {} to {} is blocked, because the target is disabled", sourcePeer.peerId(), targetPeerId);
                }
                logger.info("{} sending voteRequests to {}. {}", sourcePeer.peerId(), targetPeerId, request);
                peer.racoon.inboundEvents().voteRequests().onNext(request);
            }
        });
        this.receivedEvents.voteResponse().subscribe(response -> {
            var sourcePeer = this.peers.get(response.sourcePeerId());
            if (sourcePeer == null) {
                logger.warn("No source peer for response {}", response);
                return;
            }
            if (!sourcePeer.enabled()) {
                logger.info("Response from {} is blocked, because the source is disabled", sourcePeer.peerId());
                return;
            }
            var targetPeerId = response.destinationPeerId();
            for (var peer : this.peers.values()) {
                if (UuidTools.equals(peer.peerId(), sourcePeer.peerId())) {
                    continue;
                }
                if (targetPeerId != null && UuidTools.notEquals(targetPeerId, peer.peerId())) {
                    continue;
                }
                if (!peer.enabled()) {
                    logger.info("voteResponse from {} to {} is blocked, because the target is disabled", sourcePeer.peerId(), targetPeerId);
                }
                logger.info("{} sending voteResponse to {}. {}", sourcePeer.peerId(), targetPeerId, response);
                peer.racoon.inboundEvents().voteResponse().onNext(response);
            }
        });
    }

    public void add(UUID peerId, Raccoon racoon) {
        var item = new Peer(
                peerId,
                racoon,
                true
        );
        this.peers.put(peerId, item);
        racoon.outboundEvents().subscribe(InboundEvents.createFrom(this.receivedEvents));
        racoon.joinedRemotePeerId().subscribe(joinedRemotePeerId -> {
            logger.info("{} reported a remote racoon {} joined to the mesh", racoon.getId(), joinedRemotePeerId);
        });
        racoon.detachedRemotePeerId().subscribe(detachedRemotePeerId -> {
            logger.info("{} reported a remote racoon {} detached from the mesh", racoon.getId(), detachedRemotePeerId);
        });
        logger.info("Added {} to the router", peerId);
    }

    public void enable() {
        this.enabled = true;
    }

    public void enable(UUID id) {
        logger.info("Enabling {}", id);
        var peer = this.peers.get(id);
        if (peer != null) {
            this.peers.put(id, new Peer(
                    id,
                    peer.racoon(),
                    true
            ));
        } else {
            logger.warn("Transport with id {} does not exists", id);
        }
    }

    public void disable() {
        this.enabled = false;
    }

    public void disable(UUID id) {
        logger.info("Disabling {}", id);
        var peer = this.peers.get(id);
        if (peer != null) {
            this.peers.put(peer.peerId(), new Peer(
                    id,
                    peer.racoon(),
                    false
            ));
        } else {
            logger.warn("Transport with id {} does not exists", id);
        }
    }

    private record Peer(UUID peerId, Raccoon racoon, boolean enabled) {

    }
}