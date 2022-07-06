package io.github.balazskreith.hamok.raccoons.events;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OutboundEvents {

    static final Logger logger = LoggerFactory.getLogger(OutboundEvents.class);

    static OutboundEvents createFrom(Events events) {
        return new OutboundEvents() {
            @Override
            public Observable<RaftVoteResponse> voteResponse() {
                return events.voteResponse();
            }

            @Override
            public Observable<RaftVoteRequest> voteRequests() {
                return events.voteRequests();
            }

            @Override
            public Observable<RaftAppendEntriesRequestChunk> appendEntriesRequest() {
                return events.appendEntriesRequestChunk();
            }

            @Override
            public Observable<RaftAppendEntriesResponse> appendEntriesResponse() {
                return events.appendEntriesResponse();
            }

            @Override
            public Observable<HelloNotification> helloNotifications() {
                return events.helloNotifications();
            }

            @Override
            public Observable<EndpointStatesNotification> endpointStateNotifications() {
                return events.endpointStateNotifications();
            }
        };
    }

    Observable<RaftVoteResponse> voteResponse();
    Observable<RaftVoteRequest> voteRequests();
    Observable<RaftAppendEntriesRequestChunk> appendEntriesRequest();
    Observable<RaftAppendEntriesResponse> appendEntriesResponse();
    Observable<HelloNotification> helloNotifications();
    Observable<EndpointStatesNotification> endpointStateNotifications();

    default OutboundEvents observeOn(Scheduler scheduler) {
        var result = new Events();
        this.voteRequests().observeOn(scheduler).subscribe(result.voteRequests());
        this.voteResponse().observeOn(scheduler).subscribe(result.voteResponse());
        this.appendEntriesRequest().observeOn(scheduler).subscribe(result.appendEntriesRequestChunk());
        this.appendEntriesResponse().observeOn(scheduler).subscribe(result.appendEntriesResponse());
        this.helloNotifications().observeOn(scheduler).subscribe(result.helloNotifications());
        this.endpointStateNotifications().observeOn(scheduler).subscribe(result.endpointStateNotifications());
        return OutboundEvents.createFrom(result);
    }

    default void subscribe(InboundEvents events) {
        this.voteRequests().subscribe(events.voteRequests());
        this.voteResponse().subscribe(events.voteResponse());
        this.appendEntriesRequest().subscribe(events.appendEntriesRequests());
        this.appendEntriesResponse().subscribe(events.appendEntriesResponses());
        this.helloNotifications().subscribe(events.helloNotifications());
        this.endpointStateNotifications().subscribe(events.endpointStateNotifications());
    }
}
