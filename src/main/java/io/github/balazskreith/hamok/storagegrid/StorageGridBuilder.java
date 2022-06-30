package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.raft.LogEntry;
import io.github.balazskreith.hamok.raft.RaftConfig;
import io.github.balazskreith.hamok.raft.RxRaft;
import io.github.balazskreith.hamok.rxutils.RxCollector;
import io.github.balazskreith.hamok.rxutils.RxEmitter;
import io.github.balazskreith.hamok.storagegrid.discovery.Discovery;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;

public class StorageGridBuilder {

    private UUID id = UUID.randomUUID();
    private int endpointMaxIdleTimeInMs = 3000;
    private int propagateInactiveEndpointTimeInMs = 10000;
    private int endpointStateNotificationPeriodInMs = 1500;
    private int requestTimeoutInMs = 5000;

    private int collectOutboundMessageMaxTimeInMs = 50;
    private int collectOutboundMessageMaxItems = 1000;
    private int collectInboundMessageMaxTimeInMs = 50;
    private int collectInboundMessageMaxItems = 1000;

    private int raftElectionTimeoutInMs = 500;
    private int raftElectionMaxRandomOffsetInMs = 1000;
    private int raftHeartbeatInMs = 300;
    private int raftApplicationCommittedSyncTimeoutInMs = 0;
    private Map<Integer, LogEntry> raftLogsMap = null;
    private int raftLogsExpirationTimeoutInMs = 60000;
    private Scheduler raftScheduler = null;
    private Executor raftExecutor = null;
    private Codec<Message, byte[]> messageCodec;
    private boolean autoJoin = true;
    private String context;

    public StorageGridBuilder withContext(String value) {
        this.context = value;
        return this;
    }

    StorageGridBuilder() {
        Mapper<Message, byte[]> encoder = JsonUtils::objectToBytes;
        Mapper<byte[], Message> decoder = bytes -> JsonUtils.<Message>bytesToObject(bytes, Message.class);
        this.messageCodec = Codec.<Message, byte[]>create(
                encoder,
                decoder
        );
    }

    public StorageGridBuilder withAutoJoin(boolean value) {
        this.autoJoin = value;
        return this;
    }

    public StorageGrid build() {
        Objects.requireNonNull(this.messageCodec, "Codec for message must be given");
        var sender = RxCollector.<byte[]>builder()
                .withMaxItems(this.collectOutboundMessageMaxItems)
                .withMaxTimeInMs(this.collectOutboundMessageMaxTimeInMs)
                .build();
        var receiver = RxEmitter.<byte[]>builder()
                .withMaxItems(this.collectInboundMessageMaxItems)
                .withMaxTimeInMs(this.collectInboundMessageMaxTimeInMs)
                .build();
        var raftConfig = this.createRaftConfig();
        var raft = RxRaft.builder()
                .withConfig(raftConfig)
                .withLogsExpirationTimeInMs(this.raftLogsExpirationTimeoutInMs)
                .withLogsMap(this.raftLogsMap)
                .withScheduler(this.raftScheduler)
                .withExecutor(this.raftExecutor)
                .build();

        var discovery = Discovery.builder()
                .withLocalEndpointId(this.id)
                .withMaxIdleRemoteEndpointId(this.endpointMaxIdleTimeInMs)
                .build();

        var config = this.createStorageGridConfig();
        var result = new StorageGrid(
                config,
                receiver,
                sender,
                raft,
                discovery,
                this.messageCodec,
                this.context
        );
        if (this.autoJoin) {
            result.join();
        }
        return result;
    }

    private StorageGridConfig createStorageGridConfig() {
        return new StorageGridConfig(
                this.id,
                this.requestTimeoutInMs
        );
    }

    private RaftConfig createRaftConfig() {
        return new RaftConfig(
                this.raftElectionTimeoutInMs,
                this.raftElectionMaxRandomOffsetInMs,
                this.raftHeartbeatInMs,
                this.raftApplicationCommittedSyncTimeoutInMs,
                this.id
        );
    }
}
