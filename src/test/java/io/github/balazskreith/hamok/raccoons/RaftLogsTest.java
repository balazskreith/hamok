package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RaftLogsTest {

    private static final Logger logger = LoggerFactory.getLogger(RaftLogsTest.class);

    private RaccoonRouter router = new RaccoonRouter();
    private Raccoon leader;
    private Raccoon follower_1;
    private Raccoon follower_2;

    @Test
    @Order(1)
    @DisplayName("Setup one leader and two followers")
    void test_1() throws InterruptedException, ExecutionException, TimeoutException {
        var raccoons = List.of(
                Raccoon.builder().withLogsExpirationTimeInMs(10000).withConfig(this.createConfig()).build(),
                Raccoon.builder().withLogsExpirationTimeInMs(10000).withConfig(this.createConfig()).build()
        );

        CompletableFuture future = new CompletableFuture();
        raccoons.forEach(r -> this.router.add(r.getId(), r));
        raccoons.forEach(r -> r.changedState().filter(s -> s == RaftState.LEADER).subscribe(t -> {
            future.complete(t);
        }));
        raccoons.forEach(Raccoon::start);

        future.get(30000, TimeUnit.MILLISECONDS);

        this.leader = raccoons.stream().filter(r -> r.getState() == RaftState.LEADER).findFirst().get();
        var followers = raccoons.stream().filter(r -> r.getState() == RaftState.FOLLOWER).collect(Collectors.toList());
        Assertions.assertEquals(1, followers.size());

        this.follower_1 = followers.get(0);
    }

    @Test
    @Order(2)
    @DisplayName("Entries are submitted longer than the expiration time setup")
    void test_2() throws InterruptedException, ExecutionException, TimeoutException {
        var started = Instant.now().toEpochMilli();
        for (int i = 0; true; ++i) {
            var future = new CompletableFuture<Message>();
            var d = this.follower_1.committedEntries().map(LogEntry::entry).subscribe(future::complete);
            var testString = String.format("test-%d", i);
            var message = new Message();
            message.type = testString;
            this.leader.submit(message);

            future.get(1000, TimeUnit.MILLISECONDS);
            var now = Instant.now().toEpochMilli();
            if (15000 < now - started) {
                break;
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("When a new node joins then it requires a commit sync")
    void test_3() throws InterruptedException, ExecutionException, TimeoutException {

        this.follower_2 = Raccoon.builder().withLogsExpirationTimeInMs(10000).withConfig(this.createConfig()).build();

        this.router.add(this.follower_2.getId(), this.follower_2);
        var future = new CompletableFuture<Integer>();
        future.thenRunAsync(() -> {
            this.follower_2.setCommitIndex(this.leader.getCommitIndex());
        });
        this.follower_2.commitIndexSyncRequests().subscribe(future::complete);

        this.follower_2.start();

        Thread.sleep(10000);
    }

    private RaccoonConfig createConfig() {
        return new RaccoonConfig(
                UUID.randomUUID(),
                1000,
                1000,
                300,
                1000,
                1500,
                10000,
                true
        );
    }
}