package io.github.balazskreith.hamok.raccoons.events;

import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Consumer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Events implements Disposable {

    private static final Logger logger = LoggerFactory.getLogger(Events.class);

//    private final
    private final Subject<RaftVoteResponse> voteResponse = PublishSubject.<RaftVoteResponse>create().toSerialized();
    private final Subject<RaftVoteRequest> voteRequests = PublishSubject.<RaftVoteRequest>create().toSerialized();
    private final Subject<RaftAppendEntriesRequestChunk> appendEntriesRequest = PublishSubject.<RaftAppendEntriesRequestChunk>create().toSerialized();
    private final Subject<RaftAppendEntriesResponse> appendEntriesResponse = PublishSubject.<RaftAppendEntriesResponse>create().toSerialized();
    private final Subject<HelloNotification> helloNotifications = PublishSubject.<HelloNotification>create().toSerialized();
    private final Subject<EndpointStatesNotification> endpointStateNotifications = PublishSubject.<EndpointStatesNotification>create().toSerialized();

    public Subject<RaftAppendEntriesRequestChunk> appendEntriesRequestChunk() {
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

    public Subject<HelloNotification> helloNotifications() {
        return helloNotifications;
    }

    public Subject<EndpointStatesNotification> endpointStateNotifications() {
        return endpointStateNotifications;
    }

    public Events observeFrom(Events events) {
        events.voteRequests.subscribe(this.voteRequests);
        events.voteResponse.subscribe(this.voteResponse);
        events.appendEntriesRequest.subscribe(this.appendEntriesRequest);
        events.appendEntriesResponse.subscribe(this.appendEntriesResponse);
        events.helloNotifications.subscribe(this.helloNotifications);
        events.endpointStateNotifications.subscribe(this.endpointStateNotifications);
        return this;
    }

    public Events subscribeTo(Events events) {
        this.voteRequests.subscribe(events.voteRequests);
        this.voteResponse.subscribe(events.voteResponse);
        this.appendEntriesRequest.subscribe(events.appendEntriesRequest);
        this.appendEntriesResponse.subscribe(events.appendEntriesResponse);
        this.helloNotifications.subscribe(events.helloNotifications);
        this.endpointStateNotifications.subscribe(events.endpointStateNotifications);
        return events;
    }

    public Events doOnError(Consumer<Throwable> errorConsumer) {
        var events = new Events();
        this.voteRequests.doOnError(errorConsumer).subscribe(events.voteRequests);
        this.voteResponse.doOnError(errorConsumer).subscribe(events.voteResponse);
        this.appendEntriesRequest.doOnError(errorConsumer).subscribe(events.appendEntriesRequest);
        this.appendEntriesResponse.doOnError(errorConsumer).subscribe(events.appendEntriesResponse);
        this.helloNotifications.doOnError(errorConsumer).subscribe(events.helloNotifications);
        this.endpointStateNotifications.doOnError(errorConsumer).subscribe(events.endpointStateNotifications);
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
