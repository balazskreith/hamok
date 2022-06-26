package io.github.balazskreith.vstorage.raft;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RaftEntriesTest {
    private static final Logger logger = LoggerFactory.getLogger(RaftEntriesTest.class);

    @Test
    @Order(1)
    @DisplayName("Submitted entries are committed to participants")
    void test_1() throws InterruptedException, ExecutionException, TimeoutException {
        var participants = new RaftSimulator(3, this.createConfig());
        var leader = participants.startAndWaitForLeader();

        Assertions.assertNotNull(leader);

        for (int i = 0; i < 10; ++i) {
            var testString = String.format("test-%d", i);
            leader.submit(testString.getBytes(StandardCharsets.UTF_8));
            var futures = participants.getFollowers().stream().collect(Collectors.toMap(
                    follower -> follower.getId(),
                    follower -> new CompletableFuture<byte[]>()
            ));

            participants.getFollowers().forEach(follower -> {
                var future = futures.get(follower.getId());
                var disposable = follower.committedEntries().subscribe(logEntry -> {
                    logger.info("LogEntry {} is committed", logEntry.index());
                    future.complete(logEntry.entry());
                });
                future.thenRun(() -> disposable.dispose());
            });
            for (var it = futures.values().iterator(); it.hasNext(); ) {
                var receivedBytes = it.next();
                Assertions.assertEquals(testString, new String(receivedBytes.get(1000, TimeUnit.MILLISECONDS)));
            }
        }
    }

    private RaftConfig createConfig() {
        return new RaftConfig(
                300,
                500,
                100,
                10000,
                UUID.randomUUID()
        );
    }
}
