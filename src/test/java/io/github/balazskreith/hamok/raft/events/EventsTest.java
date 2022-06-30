package io.github.balazskreith.hamok.raft.events;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class EventsTest {

    @Test
    void shouldSendRaftVoteRequest_1() throws ExecutionException, InterruptedException, TimeoutException {
        var events = new Events();
        var event = new RaftVoteRequest(null, 0, null,0, 0);
        var future = new CompletableFuture<RaftVoteRequest>();
        events.voteRequests().subscribe(future::complete);
        events.voteRequests().onNext(event);

        Assertions.assertEquals(future.get(1000, TimeUnit.MILLISECONDS), event);
    }

    @Test
    void shouldSendRaftVoteRequest_2() throws ExecutionException, InterruptedException, TimeoutException {
        var events1 = new Events();
        var events2 = new Events();
        var event = new RaftVoteRequest(null, 0, null,0, 0);
        var future = new CompletableFuture<RaftVoteRequest>();
        events1.subscribeTo(events2);
        events2.voteRequests().subscribe(future::complete);
        events1.voteRequests().onNext(event);

        Assertions.assertEquals(future.get(1000, TimeUnit.MILLISECONDS), event);
    }

}