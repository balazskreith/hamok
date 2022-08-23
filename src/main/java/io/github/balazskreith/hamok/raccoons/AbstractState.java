package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.common.SetUtils;
import io.github.balazskreith.hamok.raccoons.events.*;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

abstract class AbstractState implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AbstractState.class);

    protected Raccoon base;

    protected AbstractState(Raccoon base) {
        this.base = base;
    }

    protected UUID getLocalPeerId() {
        return this.config().id();
    }

    public abstract RaftState getState();

    public abstract boolean submit(Message message);

    abstract void start();

    abstract void receiveVoteRequested(RaftVoteRequest request);
    abstract void receiveVoteResponse(RaftVoteResponse response);
    abstract void receiveRaftAppendEntriesRequestChunk(RaftAppendEntriesRequestChunk request);
    abstract void receiveRaftAppendEntriesResponse(RaftAppendEntriesResponse request);
    abstract void receiveHelloNotification(HelloNotification notification);
    abstract void receiveEndpointNotification(EndpointStatesNotification notification);

    protected void sendVoteRequest(RaftVoteRequest request) {
        this.base.outboundEvents.voteRequests().onNext(request);
    }

    protected void commitLogEntry(LogEntry logEntry) {
        this.base.committedEntries.onNext(logEntry);
    }

    protected void sendVoteResponse(RaftVoteResponse response) {
        this.base.outboundEvents.voteResponse().onNext(response);
    }


    protected void sendAppendEntriesRequestChunk(RaftAppendEntriesRequestChunk request) {
        this.base.outboundEvents.appendEntriesRequestChunk().onNext(request);
    }


    protected void sendAppendEntriesResponse(RaftAppendEntriesResponse response) {
        this.base.outboundEvents.appendEntriesResponse().onNext(response);
    }

    protected void sendHelloNotification(HelloNotification notification) {
        this.base.outboundEvents.helloNotifications().onNext(notification);
    }

    protected void lead() {
        this.base.changeState(new LeaderState(this.base));
    }

    protected void follow() {
        this.follow(0);
    }

    protected void follow(int timedOutElection) {
        this.base.changeState(new FollowerState(this.base, timedOutElection));
    }

    protected void elect(int prevTimedOutElection) {
        this.base.changeState(new CandidateState(this.base, prevTimedOutElection));
    }

    @Override
    public String toString() {
        return this.getState().name();
    }

    protected RaftLogs logs() {
        return this.base.logs;
    }

    protected SyncedProperties syncedProperties() {
        return this.base.syncProperties;
    }

    protected RaccoonConfig config() {
        return this.base.config;
    }

    protected RemotePeers remotePeers() {
        return this.base.remotePeers;
    }

    protected void setActualLeaderId(UUID leaderId) {
        this.base.setActualLeaderId(leaderId);
    }

    protected UUID getLeaderId() {
        return this.base.getLeaderId();
    }

    protected CompletableFuture<Boolean> requestStorageSync() {
        return this.base.requestStorageSync();
    }

    protected void inactivatedLocalPeerId() {
        this.base.signalInactivatedLocalPeer();
    }

    protected void sendEndpointStateNotification(Set<UUID> remotePeerIds) {
        if (remotePeerIds == null || remotePeerIds.size() < 1) {
            return;
        }

        var remotePeers = this.remotePeers();
        var activePeerIds = SetUtils.combineAll(remotePeers.getActiveRemotePeerIds(), Set.of(this.getLocalPeerId()));
        var logs = logs();
        for (var remotePeerId : remotePeerIds) {
            var notification = new EndpointStatesNotification(
                    this.getLocalPeerId(),
                    activePeerIds,
                    logs.size(),
                    logs.getNextIndex(),
                    logs.getCommitIndex(),
                    remotePeerId,
                    syncedProperties().currentTerm.get()
            );
            this.base.outboundEvents.endpointStateNotifications().onNext(notification);

        }

    }
}
