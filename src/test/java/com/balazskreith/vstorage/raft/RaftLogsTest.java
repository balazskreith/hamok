package com.balazskreith.vstorage.raft;

import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RaftLogsTest {

    private static final Logger logger = LoggerFactory.getLogger(RxRaftElectionTest.class);

    private RxRaft leader;
    private RxRaft follower_1;
    private RxRaft follower_2;
    private RxRaft stopped;

    @Test
    @Order(1)
    @DisplayName("First the cluster elect the leader")
    void test_1() throws InterruptedException {
        logger.info("\n\n ---- Test 1 ----- \n\n");
        var raft_1 = RxRaft.builder().withLogsExpirationTimeInMs(2000).withConfig(this.createConfig()).build();
        var raft_2 = RxRaft.builder().withLogsExpirationTimeInMs(2000).withConfig(this.createConfig()).build();

        raft_1.transport().sender().observeOn(Schedulers.io()).filterAndSubscribe(raft_2.getId(), raft_2.transport().receiver());
        raft_2.transport().sender().observeOn(Schedulers.io()).filterAndSubscribe(raft_1.getId(), raft_1.transport().receiver());
        raft_1.addPeerId(raft_2.getId());
        raft_2.addPeerId(raft_1.getId());

        raft_1.start();
        raft_2.start();

        Thread.sleep(10000);

        this.leader = raft_1.getState() == RaftState.LEADER ? raft_1 : raft_2;
        this.follower_1 = raft_1.getState() == RaftState.FOLLOWER ? raft_1 : raft_2;
    }

    @Test
    @Order(2)
    @DisplayName("Entries are submitted longer than the expiration time setup")
    void test_2() throws InterruptedException, ExecutionException, TimeoutException {
        var started = Instant.now().toEpochMilli();
        for (int i = 0; true; ++i) {
            var future = new CompletableFuture<byte[]>();
            var d = this.follower_1.committedEntries().map(LogEntry::entry).subscribe(future::complete);
            var testString = String.format("test-%d", i);
            this.leader.submit(testString.getBytes(StandardCharsets.UTF_8));

            future.get(1000, TimeUnit.MILLISECONDS);
            var now = Instant.now().toEpochMilli();
            if (5000 < now - started) {
                break;
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("When a new node joins then it requires a commit sync")
    void test_3() throws InterruptedException, ExecutionException, TimeoutException {
        logger.info("\n\n ---- Test 3 ----- \n\n");
        var newPeer = RxRaft.builder().withConfig(this.createConfig()).build();
        newPeer.transport().sender().observeOn(Schedulers.io()).filterAndSubscribe(this.leader.getId(), this.leader.transport().receiver());
        newPeer.transport().sender().observeOn(Schedulers.io()).filterAndSubscribe(this.follower_1.getId(), this.follower_1.transport().receiver());

        this.leader.transport().sender().filterAndSubscribe(newPeer.getId(), newPeer.transport().receiver());
        this.follower_1.transport().sender().filterAndSubscribe(newPeer.getId(), newPeer.transport().receiver());
        newPeer.addPeerId(this.leader.getId(), this.follower_1.getId());
        this.leader.addPeerId(newPeer.getId());
        this.follower_1.addPeerId(newPeer.getId());

        var future = new CompletableFuture<Integer>();
        newPeer.commitIndexSyncRequests().subscribe(future::complete);

        this.follower_1.stop();// remove it after we figured out the problem
        this.leader.removePeerId(this.follower_1.getId());
        newPeer.start();

        future.thenRunAsync(() -> {
            newPeer.setCommitIndex(this.leader.getCommitIndex());
        });


        Thread.sleep(10000);
    }

    private RaftConfig createConfig() {
        return new RaftConfig(
                1000,
                500,
                300,
                10000,
                UUID.randomUUID()
        );
    }
}