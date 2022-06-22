package com.balazskreith.vstorage.raft.events;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public interface EventSender {

    static final Logger logger = LoggerFactory.getLogger(EventSender.class);

    static EventSender createFrom(Events events) {
        return new EventSender() {
            @Override
            public Observable<RaftVoteResponse> voteResponse() {
                return events.voteResponse();
            }

            @Override
            public Observable<RaftVoteRequest> voteRequests() {
                return events.voteRequests();
            }

            @Override
            public Observable<RaftAppendEntriesRequest> appendEntriesRequest() {
                return events.appendEntriesRequest();
            }

            @Override
            public Observable<RaftAppendEntriesResponse> appendEntriesResponse() {
                return events.appendEntriesResponse();
            }
        };
    }



    Observable<RaftVoteResponse> voteResponse();
    Observable<RaftVoteRequest> voteRequests();
    Observable<RaftAppendEntriesRequest> appendEntriesRequest();
    Observable<RaftAppendEntriesResponse> appendEntriesResponse();

    default EventSender observeOn(Scheduler scheduler) {
        var result = new Events();
        this.voteRequests().observeOn(scheduler).subscribe(result.voteRequests());
        this.voteResponse().observeOn(scheduler).subscribe(result.voteResponse());
        this.appendEntriesRequest().observeOn(scheduler).subscribe(result.appendEntriesRequest());
        this.appendEntriesResponse().observeOn(scheduler).subscribe(result.appendEntriesResponse());
        return EventSender.createFrom(result);
    }

    default void subscribe(EventReceiver events) {
        this.voteRequests().subscribe(events.voteRequests());
        this.voteResponse().subscribe(events.voteResponse());
        this.appendEntriesRequest().subscribe(events.appendEntriesRequest());
        this.appendEntriesResponse().subscribe(events.appendEntriesResponse());
    }

    default void printAndSubscribe(EventReceiver events) {
        this.voteRequests().map(Events::printAndForward).subscribe(events.voteRequests());
        this.voteResponse().map(Events::printAndForward).subscribe(events.voteResponse());
        this.appendEntriesRequest().map(Events::printAndForward).subscribe(events.appendEntriesRequest());
        this.appendEntriesResponse().map(Events::printAndForward).subscribe(events.appendEntriesResponse());
    }

    default void filterAndSubscribe(UUID destinationPeerId, EventReceiver events) {
        this.voteRequests().filter(event -> event.peerId() == destinationPeerId).subscribe(events.voteRequests());
        this.voteResponse().filter(event -> event.destinationPeerId() == destinationPeerId).subscribe(events.voteResponse());
        this.appendEntriesRequest().filter(event -> event.peerId() == destinationPeerId).subscribe(events.appendEntriesRequest());
        this.appendEntriesResponse().filter(event -> event.destinationPeerId() == destinationPeerId).subscribe(events.appendEntriesResponse());
    }
}
