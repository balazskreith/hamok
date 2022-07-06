package io.github.balazskreith.hamok.raccoons;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// I tried to be funny... sorry uncle Bob, I think I need a vacation.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RacoonTest {
    private static final Logger logger = LoggerFactory.getLogger(RacoonTest.class);

    private RaccoonRouter router = new RaccoonRouter();
    private Raccoon leader;
    private Raccoon follower_1;
    private Raccoon follower_2;
    private Raccoon stopped;

    @Test
    @Order(1)
    @DisplayName("When only one Racoon is up Then no leader is elected")
    void test_1() throws InterruptedException {
        logger.info("\n\n ---- Test 1 ----- \n\n");
        var racoon = Raccoon.builder()
                .withConfig(this.createConfig())
                .build();
        AtomicInteger changedState = new AtomicInteger();
        AtomicReference leaderId = new AtomicReference(null);

        racoon.start();

        Thread.sleep(5000);

        var state = racoon.getState();
        racoon.stop();

        Assertions.assertEquals(state, RaftState.FOLLOWER);
    }

    @Test
    @Order(2)
    @DisplayName("When two racoons communicate one become leader and the other become follower")
    void test_2() throws InterruptedException {
        logger.info("\n\n ---- Test 2 ----- \n\n");
        var racoon_1 = Raccoon.builder().withConfig(this.createConfig()).build();
        var racoon_2 = Raccoon.builder().withConfig(this.createConfig()).build();

        this.router.add(racoon_1.getId(), racoon_1);
        this.router.add(racoon_2.getId(), racoon_2);

        racoon_1.start();
        racoon_2.start();

        Thread.sleep(10000);

        this.leader = racoon_1.getState() == RaftState.LEADER ? racoon_1 : racoon_2;
        this.follower_1 = racoon_1.getState() == RaftState.FOLLOWER ? racoon_1 : racoon_2;

        Assertions.assertTrue(this.leader.getState() == RaftState.LEADER);
        Assertions.assertTrue(this.follower_1.getState() == RaftState.FOLLOWER);
    }

    @Test
    @Order(3)
    @DisplayName("When a racoon joins to the mesh, it become a follower and no new election starts")
    void test_3() throws InterruptedException {
        logger.info("\n\n ---- Test 3 ----- \n\n");
        var newRacoon = Raccoon.builder().withConfig(this.createConfig()).build();

        this.router.add(newRacoon.getId(), newRacoon);

        newRacoon.start();

        Thread.sleep(5000);

        Assertions.assertTrue(newRacoon.getState() == RaftState.FOLLOWER);

        this.follower_2 = newRacoon;
    }

    @Test
    @Order(4)
    @DisplayName("When a leader racoon becomes unavailable, Then the remaining racoons select a new leader")
    void test_4() throws InterruptedException {
        logger.info("\n\n ---- Test 4 ----- \n\n");
        this.stopped = this.leader;
        this.router.disable(this.stopped.getId());

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
    @DisplayName("When a racoon, which was the leader but become unavailable comes back to mesh Then it becomes a follower ")
    void test_5() throws InterruptedException {
        logger.info("\n\n ---- Test 5 ----- \n\n");
        this.router.enable(this.stopped.getId());

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
    @DisplayName("When a leader racoon is kicked out, Then the remaining racoons select a new leader")
    void test_6() throws InterruptedException {
        logger.info("\n\n ---- Test 6 ----- \n\n");
        this.stopped = this.leader;
        this.stopped.stop();

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
    @Order(7)
    @DisplayName("When a kicked out racoon wakes up Then it becomes a follower")
    void test_7() throws InterruptedException {
        logger.info("\n\n ---- Test 7 ----- \n\n");
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