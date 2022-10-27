package io.github.balazskreith.hamok.raccoons;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// I tried to be funny... sorry uncle Bob, I think I need a vacation.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RacoonTest {
    private static final Logger logger = LoggerFactory.getLogger(RacoonTest.class);

    private RaccoonRouter router = new RaccoonRouter();

    @Test
    @Order(1)
    @DisplayName("When only one Racoon is up Then no leader is elected")
    void test_1() throws InterruptedException {
    }

    @Test
    @Order(2)
    @DisplayName("When two racoons communicate one become leader and the other become follower")
    void test_2() throws InterruptedException {
    }

    @Test
    @Order(3)
    @DisplayName("When a racoon joins to the mesh, it become a follower and no new election starts")
    void test_3() throws InterruptedException {
    }

    @Test
    @Order(4)
    @DisplayName("When a leader racoon becomes unavailable, Then the remaining racoons select a new leader")
    void test_4() throws InterruptedException {
    }

    @Test
    @Order(5)
    @DisplayName("When a racoon, which was the leader but become unavailable comes back to mesh Then it becomes a follower ")
    void test_5() throws InterruptedException {
    }

    @Test
    @Order(6)
    @DisplayName("When a leader racoon is kicked out, Then the remaining racoons select a new leader")
    void test_6() throws InterruptedException {
    }

    @Test
    @Order(7)
    @DisplayName("When a kicked out racoon wakes up Then it becomes a follower")
    void test_7() throws InterruptedException {
    }

}