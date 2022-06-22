package com.balazskreith.vstorage.raft.events;

import com.balazskreith.vstorage.common.JsonUtils;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Events implements Disposable {

    private static final Logger logger = LoggerFactory.getLogger(Events.class);

    static<T> T printAndForward(T obj) {
        logger.info("{}: {}", obj.getClass().getSimpleName(), JsonUtils.objectToString(obj));
        return obj;
    }

    private final Subject<RaftVoteResponse> voteResponse = PublishSubject.<RaftVoteResponse>create().toSerialized();
    private final Subject<RaftVoteRequest> voteRequests = PublishSubject.<RaftVoteRequest>create().toSerialized();
    private final Subject<RaftAppendEntriesRequest> appendEntriesRequest = PublishSubject.<RaftAppendEntriesRequest>create().toSerialized();
    private final Subject<RaftAppendEntriesResponse> appendEntriesResponse = PublishSubject.<RaftAppendEntriesResponse>create().toSerialized();

    public Subject<RaftAppendEntriesRequest> appendEntriesRequest() {
        return appendEntriesRequest;
    }

    public Subject<RaftAppendEntriesResponse> appendEntriesResponse() {
        return appendEntriesResponse;
    }

    public Subject<RaftVoteRequest> voteRequests() {
        return voteRequests;
    }

    public Subject<RaftVoteResponse> voteResponse() {
        return voteResponse;
    }

    public Events observeFrom(Events events) {
        events.voteRequests.subscribe(this.voteRequests);
        events.voteResponse.subscribe(this.voteResponse);
        events.appendEntriesRequest.subscribe(this.appendEntriesRequest);
        events.appendEntriesResponse.subscribe(this.appendEntriesResponse);
        return this;
    }

    public Events subscribeTo(Events events) {
        this.voteRequests.subscribe(events.voteRequests);
        this.voteResponse.subscribe(events.voteResponse);
        this.appendEntriesRequest.subscribe(events.appendEntriesRequest);
        this.appendEntriesResponse.subscribe(events.appendEntriesResponse);
        return events;
    }

    public Events printAndSubscribeTo(Events events) {
        this.voteRequests.map(Events::printAndForward).subscribe(events.voteRequests);
        this.voteResponse.map(Events::printAndForward).subscribe(events.voteResponse);
        this.appendEntriesRequest.map(Events::printAndForward).subscribe(events.appendEntriesRequest);
        this.appendEntriesResponse.map(Events::printAndForward).subscribe(events.appendEntriesResponse);
        return events;
    }

    public Events doOnError(Consumer<Throwable> errorConsumer) {
        var events = new Events();
        this.voteRequests.doOnError(errorConsumer).subscribe(events.voteRequests);
        this.voteResponse.doOnError(errorConsumer).subscribe(events.voteResponse);
        this.appendEntriesRequest.doOnError(errorConsumer).subscribe(events.appendEntriesRequest);
        this.appendEntriesResponse.doOnError(errorConsumer).subscribe(events.appendEntriesResponse);
        return events;
    }

    @Override
    public void dispose() {
        List.of(this.appendEntriesRequest, this.voteRequests).stream()
                .filter(s -> !s.hasComplete() && !s.hasThrowable())
                .forEach(Subject::onComplete);
    }

    @Override
    public boolean isDisposed() {
        return List.of(this.appendEntriesRequest, this.voteRequests).stream()
                .allMatch(s -> s.hasThrowable() || s.hasComplete());
    }
}
