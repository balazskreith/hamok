package io.github.balazskreith.vstorage.raft;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RxRaftElectionTest {

    private static final Logger logger = LoggerFactory.getLogger(RxRaftElectionTest.class);

    private RxRaft leader;
    private RxRaft follower_1;
    private RxRaft follower_2;
    private RxRaft stopped;

    @Test
    @Order(1)
    @DisplayName("When only one Raft is up then no leader is elected")
    void test_1() throws InterruptedException {
        logger.info("\n\n ---- Test 1 ----- \n\n");
        var raft = RxRaft.builder()
                .withConfig(this.createConfig())
                .build();
        AtomicInteger changedState = new AtomicInteger();
        AtomicReference leaderId = new AtomicReference(null);

        raft.changedLeaderId().subscribe(leaderId::set);
        raft.changedState().subscribe(state -> changedState.incrementAndGet());
        raft.start();

        Thread.sleep(5000);
        raft.stop();

        Assertions.assertNull(leaderId.get());
        Assertions.assertTrue(0 < changedState.get());
    }

    @Test
    @Order(2)
    @DisplayName("When two raft participant communicate one become leader and the other become follower")
    void test_2() throws InterruptedException {
        logger.info("\n\n ---- Test 2 ----- \n\n");
        var raft_1 = RxRaft.builder().withConfig(this.createConfig()).build();
        var raft_2 = RxRaft.builder().withConfig(this.createConfig()).build();

        raft_1.transport().sender().filterAndSubscribe(raft_2.getId(), raft_2.transport().receiver());
        raft_2.transport().sender().filterAndSubscribe(raft_1.getId(), raft_1.transport().receiver());


        raft_1.addPeerId(raft_2.getId());
        raft_2.addPeerId(raft_1.getId());

        raft_1.start();
        raft_2.start();

        Thread.sleep(10000);

        this.leader = raft_1.getState() == RaftState.LEADER ? raft_1 : raft_2;
        this.follower_1 = raft_1.getState() == RaftState.FOLLOWER ? raft_1 : raft_2;

        Assertions.assertTrue(this.leader.getState() == RaftState.LEADER);
        Assertions.assertTrue(this.follower_1.getState() == RaftState.FOLLOWER);
    }

    @Test
    @Order(3)
    @DisplayName("When a new peer joins to the mesh, it become a follower and no new election starts")
    void test_3() throws InterruptedException {
        logger.info("\n\n ---- Test 3 ----- \n\n");
        var newPeer = RxRaft.builder().withConfig(this.createConfig()).build();
        newPeer.transport().sender().filterAndSubscribe(this.leader.getId(), this.leader.transport().receiver());
        newPeer.transport().sender().filterAndSubscribe(this.follower_1.getId(), this.follower_1.transport().receiver());

        this.leader.transport().sender().filterAndSubscribe(newPeer.getId(), newPeer.transport().receiver());
        this.follower_1.transport().sender().filterAndSubscribe(newPeer.getId(), newPeer.transport().receiver());
        newPeer.addPeerId(this.leader.getId(), this.follower_1.getId());
        this.leader.addPeerId(newPeer.getId());
        this.follower_1.addPeerId(newPeer.getId());

        newPeer.start();

        Thread.sleep(5000);

        Assertions.assertTrue(newPeer.getState() == RaftState.FOLLOWER);

        this.follower_2 = newPeer;
    }

    @Test
    @Order(4)
    @DisplayName("When a leader falls down, a new leader is elected from one of the follower")
    void test_4() throws InterruptedException {
        logger.info("\n\n ---- Test 4 ----- \n\n");
        this.leader.stop();
        this.stopped = this.leader;

        Thread.sleep(10000);

        if (this.follower_1.getState() == RaftState.LEADER) {
            this.leader = this.follower_1;
            this.follower_1 = null;
        }
        if (this.follower_2.getState() == RaftState.LEADER) {
            this.leader = this.follower_2;
            this.follower_2 = null;
        }

        Assertions.assertNotNull(this.leader);
    }

    @Test
    @Order(5)
    @DisplayName("When a stopped node start again, it become a follower")
    void test_5() throws InterruptedException {
        logger.info("\n\n ---- Test 5 ----- \n\n");
        this.stopped.start();

        Thread.sleep(10000);

        Assertions.assertTrue(this.stopped.getState() == RaftState.FOLLOWER);

        if (this.follower_1== null) {
            this.follower_1 = this.stopped;
            this.stopped = null;
        }
        if (this.follower_2== null) {
            this.follower_2 = this.stopped;
            this.stopped = null;
        }

        Assertions.assertNull(this.stopped);
    }

    @Test
    @Order(6)
    @DisplayName("When a node is cut from the mesh it starts its own election")
    void test_6() throws InterruptedException {
        logger.info("\n\n ---- Test 6 ----- \n\n");
        this.follower_2.removePeerId(this.follower_1.getId());
        this.leader.removePeerId(this.follower_1.getId());
        this.follower_1.removePeerId(this.follower_2.getId(), this.leader.getId());

        Thread.sleep(10000);

        Assertions.assertNotEquals(RaftState.LEADER, this.follower_1.getState());
    }


    @Test
    @Order(7)
    @DisplayName("When the node got back again, no two leader appears")
    void test_7() throws InterruptedException {
        logger.info("\n\n ---- Test 7 ----- \n\n");
        this.follower_2.addPeerId(this.follower_1.getId());
        this.leader.addPeerId(this.follower_1.getId());
        this.follower_1.addPeerId(this.follower_2.getId(), this.leader.getId());

        Thread.sleep(10000);

        var list = List.of(this.follower_1, this.follower_2, this.leader);
        Assertions.assertEquals(1, list.stream().filter(r -> RaftState.LEADER.equals(r.getState())).count());
        Assertions.assertEquals(2, list.stream().filter(r -> RaftState.FOLLOWER.equals(r.getState())).count());
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