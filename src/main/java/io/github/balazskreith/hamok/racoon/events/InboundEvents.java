package io.github.balazskreith.hamok.racoon.events;

import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;

public interface InboundEvents extends Disposable {
    static InboundEvents createFrom(Events events) {
        return new InboundEvents() {
            @Override
            public void dispose() {
                events.dispose();
            }

            @Override
            public boolean isDisposed() {
                return events.isDisposed();
            }

            @Override
            public Observer<RaftVoteResponse> voteResponse() {
                return events.voteResponse();
            }

            @Override
            public Observer<RaftVoteRequest> voteRequests() {
                return events.voteRequests();
            }

            @Override
            public Observer<RaftAppendEntriesRequest> appendEntriesRequests() {
                return events.appendEntriesRequest();
            }

            @Override
            public Observer<RaftAppendEntriesResponse> appendEntriesResponses() {
                return events.appendEntriesResponse();
            }

            @Override
            public Observer<HelloNotification> helloNotifications() {
                return events.helloNotifications();
            }

            @Override
            public Observer<EndpointStatesNotification> endpointStateNotifications() {
                return events.endpointStateNotifications();
            }
        };
    }

    Observer<RaftVoteResponse> voteResponse();
    Observer<RaftVoteRequest> voteRequests();
    Observer<RaftAppendEntriesRequest> appendEntriesRequests();
    Observer<RaftAppendEntriesResponse> appendEntriesResponses();
    Observer<HelloNotification> helloNotifications();
    Observer<EndpointStatesNotification> endpointStateNotifications();

    default InboundEvents observeOn(Scheduler scheduler) {
        var result = new Events();
        result.voteRequests().observeOn(scheduler).subscribe(this.voteRequests());
        result.voteResponse().observeOn(scheduler).subscribe(this.voteResponse());
        result.appendEntriesRequest().observeOn(scheduler).subscribe(this.appendEntriesRequests());
        result.appendEntriesResponse().observeOn(scheduler).subscribe(this.appendEntriesResponses());
        result.helloNotifications().observeOn(scheduler).subscribe(this.helloNotifications());
        result.endpointStateNotifications().observeOn(scheduler).subscribe(this.endpointStateNotifications());
        return InboundEvents.createFrom(result);
    }
}
