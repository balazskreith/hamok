package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.common.Disposer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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

    private RaccoonRouter router = new RaccoonRouter();
    private Raccoon leader;
    private Raccoon follower_1;
    private Raccoon follower_2;

    @Test
    @Order(1)
    @DisplayName("Setup one leader and two followers")
    void test_1() throws InterruptedException, ExecutionException, TimeoutException {
        var raccoons = List.of(
                Raccoon.builder().withConfig(this.createConfig()).build(),
                Raccoon.builder().withConfig(this.createConfig()).build(),
                Raccoon.builder().withConfig(this.createConfig()).build()
        );

        var future = new CompletableFuture<>();
        raccoons.forEach(r -> this.router.add(r.getId(), r));
        raccoons.forEach(r -> r.changedState().filter(s -> s == RaftState.LEADER).subscribe(t -> future.complete(t)));
        raccoons.forEach(Raccoon::start);

        future.get(30000, TimeUnit.MILLISECONDS);

        this.leader = raccoons.stream().filter(r -> r.getState() == RaftState.LEADER).findFirst().get();
        var followers = raccoons.stream().filter(r -> r.getState() == RaftState.FOLLOWER).collect(Collectors.toList());
        Assertions.assertEquals(2, followers.size());

        this.follower_1 = followers.get(0);
        this.follower_2 = followers.get(1);
    }

    @Test
    @Order(2)
    @DisplayName("Submitted entries to the leader are committed to followers")
    void test_2() throws InterruptedException, ExecutionException, TimeoutException {

        Assertions.assertNotNull(leader);

        for (int i = 0; i < 10; ++i) {
            var testString = String.format("test-%d", i);
            var futures = Map.of(
                    this.follower_1.getId(), new CompletableFuture<byte[]>(),
                    this.follower_2.getId(), new CompletableFuture<byte[]>()
            );
            var entry_1 = new CompletableFuture<byte[]>();
            var entry_2 = new CompletableFuture<byte[]>();
            var disposer = Disposer.builder()
                    .addDisposable(this.follower_1.committedEntries().map(LogEntry::entry).subscribe(entry_1::complete))
                    .addDisposable(this.follower_2.committedEntries().map(LogEntry::entry).subscribe(entry_2::complete))
                    .build();

            this.leader.submit(testString.getBytes(StandardCharsets.UTF_8));

            CompletableFuture.allOf(entry_1, entry_2).get(30000, TimeUnit.MILLISECONDS);

            Assertions.assertEquals(testString, new String(entry_1.get(1000, TimeUnit.MILLISECONDS)));
            Assertions.assertEquals(testString, new String(entry_2.get(1000, TimeUnit.MILLISECONDS)));
        }
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
