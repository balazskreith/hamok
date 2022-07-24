package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.raccoons.RaccoonBuilder;
import io.github.balazskreith.hamok.raccoons.RaccoonConfig;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;

public class StorageGridBuilder {

    private UUID id = UUID.randomUUID();
    private Codec<Message, byte[]> messageCodec;
    private String context;
    private RaccoonBuilder raccoonBuilder = new RaccoonBuilder();
    private RaccoonConfig raccoonConfig = RaccoonConfig.create();
    private StorageGridConfig storageGridConfig = StorageGridConfig.create();

    public StorageGridBuilder withContext(String value) {
        this.context = value;
        return this;
    }

    public StorageGridBuilder withLocalEndpointId(UUID localEndpointId) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetId(localEndpointId);
        return this;
    }

    public StorageGridBuilder withElectionTimeoutInMs(int electionTimeoutInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetElectionTimeoutInMs(electionTimeoutInMs);
        return this;
    }

    public StorageGridBuilder withFollowerMaxIdleInMs(int followerMaxIdleInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetFollowerMaxIdleInMs(followerMaxIdleInMs);
        return this;
    }

    public StorageGridBuilder withHeartbeatInMs(int heartbeatInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetHeartbeatInMs(heartbeatInMs);
        return this;
    }

    public StorageGridBuilder withSendingHelloTimeoutInMs(int sendingHelloTimeoutInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetSendingHelloTimeoutInMs(sendingHelloTimeoutInMs);
        return this;
    }

    public StorageGridBuilder withPeerMaxIdleTimeInMs(int peerMaxIdleTimeInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetPeerMaxIdleTimeInMs(peerMaxIdleTimeInMs);
        return this;
    }

    public StorageGridBuilder withApplicationCommitIndexSyncTimeoutInMs(int applicationCommitIndexSyncTimeoutInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetApplicationCommitIndexSyncTimeoutInMs(applicationCommitIndexSyncTimeoutInMs);
        return this;
    }

    public StorageGridBuilder withAutoDiscovery(boolean autoDiscovery) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetAutoDiscovery(autoDiscovery);
        return this;
    }

    public StorageGridBuilder withRequestTimeoutInMs(int requestTimeoutInMs) {
        this.storageGridConfig = storageGridConfig.copyAndSet(requestTimeoutInMs);
        return this;
    }

    public StorageGridBuilder withRaftMaxLogRetentionTimeInMs(int retentionTimeInMs) {
        this.raccoonBuilder.withLogsExpirationTimeInMs(retentionTimeInMs);
        return this;
    }

    public StorageGridBuilder withBaseExecutor(Executor executor) {
        this.raccoonBuilder.withExecutor(executor);
        return this;
    }

    public StorageGridBuilder withBaseScheduler(Scheduler scheduler) {
        this.raccoonBuilder.withScheduler(scheduler);
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

    public StorageGrid build() {
        Objects.requireNonNull(this.messageCodec, "Codec for message must be given");
        var raccoon = this.raccoonBuilder.withConfig(this.raccoonConfig).build();
        var result = new StorageGrid(
                this.storageGridConfig,
                raccoon,
                this.messageCodec,
                this.context
        );
        return result;
    }


}
