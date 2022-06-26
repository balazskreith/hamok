package io.github.balazskreith.vstorage.raft;

import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class RaftSimulator {
    private static final Logger logger = LoggerFactory.getLogger(RaftSimulator.class);

    private final Map<UUID, RxRaft> participants = new ConcurrentHashMap<>();

    public RaftSimulator(int numberOfParticipants, RaftConfig template) {
        this(numberOfParticipants, template, 30000);
    }

    public RaftSimulator(int numberOfParticipants, RaftConfig template, int expirationTimeInMs) {
        for (int i = 0; i < numberOfParticipants; ++i) {
            var config = this.getConfig(template);
            var raft = RxRaft.builder()
                    .withLogsExpirationTimeInMs(expirationTimeInMs)
                    .withConfig(config).build();
            this.participants.put(raft.getId(), raft);
        }
        for (var it = this.participants.values().iterator(); it.hasNext(); ) {
            var source = it.next();
            for (var jt = this.participants.values().iterator(); jt.hasNext(); ) {
                var destination = jt.next();
                if (source.getId() == destination.getId()) continue;
                source.transport().sender().filterAndSubscribe(destination.getId(), destination.transport().receiver());
                logger.info("{} connected to {}", source.getId().toString().substring(0, 8), destination.getId().toString().substring(0, 8));
//                destination.transport().sender().filterAndSubscribe(source.getId(), source.transport().receiver());
                source.addPeerId(destination.getId());
            }
        }
    }

    public List<RxRaft> getFollowers() {
        return this.participants.values().stream().filter(r -> r.getState() == RaftState.FOLLOWER).collect(Collectors.toList());
    }

    public RxRaft startAndWaitForLeader() {
        this.participants.values().forEach(RxRaft::start);
        return this.wrapFutureGet(this.waitForLeader());
    }

    public Future<RxRaft> waitForLeader() {
        var result = new CompletableFuture<RxRaft>();
        Runnable timer = () -> {
            var found = this.participants.values().stream().filter(r -> r.getState() == RaftState.LEADER).findFirst();
            if (found.isPresent()) {
                result.complete(found.get());
            } else {
                result.complete(this.wrapFutureGet(this.waitForLeader()));
            }
        };
        Schedulers.single().scheduleDirect(timer, 1000, TimeUnit.MILLISECONDS);
        return result;
    }

    private<T> T wrapFutureGet(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }

    private RaftConfig getConfig(RaftConfig template) {
        return new RaftConfig(
                template.electionMinTimeoutInMs(),
                template.electionMaxRandomOffsetInMs(),
                template.heartbeatInMs(),
                template.applicationCommitIndexSyncTimeoutInMs(),
                UUID.randomUUID()
        );
    }
}
