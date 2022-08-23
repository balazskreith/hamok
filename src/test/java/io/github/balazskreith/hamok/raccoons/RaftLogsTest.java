package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.Models;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class RaftLogsTest {

    private static final Logger logger = LoggerFactory.getLogger(RaftLogsTest.class);

    private final RaftLogs logs = new RaftLogs(new HashMap<Integer, LogEntry>(), 10000);

    @Test
    @Order(1)
    void submitAndIterateLogs_1() {
        this.logs.submit(1, Models.Message.newBuilder().setType("one").build());
        this.logs.submit(1, Models.Message.newBuilder().setType("two").build());

        var collectedEntries = this.logs.collectEntries(0);
        Assertions.assertEquals("one", collectedEntries.get(0).getType());
        Assertions.assertEquals("two", collectedEntries.get(1).getType());
    }

    @Test
    @Order(2)
    void submitAndIterateLogs_2() {
        this.logs.submit(1, Models.Message.newBuilder().setType("three").build());

        var collectedEntries = this.logs.collectEntries(1);
        Assertions.assertEquals("two", collectedEntries.get(0).getType());
        Assertions.assertEquals("three", collectedEntries.get(1).getType());
    }


}