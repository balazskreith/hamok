package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.raccoons.RaccoonBuilder;
import io.github.balazskreith.hamok.raccoons.RaccoonConfig;
import io.github.balazskreith.hamok.storagegrid.messages.GridOpSerDe;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.UUID;
import java.util.concurrent.Executor;

/**
 * Builder class for Storage Grid
 */
public class StorageGridBuilder {

    private String context;
    private RaccoonBuilder raccoonBuilder = new RaccoonBuilder();
    private RaccoonConfig raccoonConfig = RaccoonConfig.create();
    private StorageGridConfig storageGridConfig = StorageGridConfig.create();
    private GridOpSerDe gridOpSerDe = new GridOpSerDe();
    private StorageGridExecutors storageGridExecutors = StorageGridExecutors.createDefault();

    /**
     * Sets the context for the grid
     * @param value the value of the context
     * @return the builder object
     */
    public StorageGridBuilder withContext(String value) {
        this.context = value;
        return this;
    }

    /**
     * Sets the Id of the local endpoint
     *
     * @param localEndpointId A UUID for local endpoint
     * @return the builder object
     */
    public StorageGridBuilder withLocalEndpointId(UUID localEndpointId) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetId(localEndpointId);
        return this;
    }

    /**
     * Sets the serialization deserialization object for grid messages
     *
     * @param gridOpSerDe the serde
     * @return the builder object
     */
    public StorageGridBuilder withGridOpSerDe(GridOpSerDe gridOpSerDe) {
        this.gridOpSerDe = gridOpSerDe;
        return this;
    }

    /**
     * Sets the timeout for Raft elections
     *
     * @param electionTimeoutInMs timeout in milliseconds for Raft being in Candidate State
     * @return the builder object
     */
    public StorageGridBuilder withElectionTimeoutInMs(int electionTimeoutInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetElectionTimeoutInMs(electionTimeoutInMs);
        return this;
    }

    /**
     * Sets the maximum amount of time for follower state to be idle in Raft
     *
     * @param followerMaxIdleInMs timeout in milliseconds
     * @return the builder object
     */
    public StorageGridBuilder withFollowerMaxIdleInMs(int followerMaxIdleInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetFollowerMaxIdleInMs(followerMaxIdleInMs);
        return this;
    }

    /**
     * Sets the ticking time of the Raft operations
     *
     * @param heartbeatInMs the heartbeat of the Raft in milliseconds
     * @return the builder object
     */
    public StorageGridBuilder withHeartbeatInMs(int heartbeatInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetHeartbeatInMs(heartbeatInMs);
        return this;
    }

    /**
     * Sets the period of which Raft sends a hello messages to other peer
     *
     * @param sendingHelloTimeoutInMs timeout in milliseconds
     * @return the builder object
     */
    public StorageGridBuilder withSendingHelloTimeoutInMs(int sendingHelloTimeoutInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetSendingHelloTimeoutInMs(sendingHelloTimeoutInMs);
        return this;
    }

    /**
     * Sets the maximum amount of time for a peer to allowed to be idle
     * before it is reported to be inactive by the leader
     *
     * @param peerMaxIdleTimeInMs timeout in milliseconds
     * @return the builder object
     */
    public StorageGridBuilder withPeerMaxIdleTimeInMs(int peerMaxIdleTimeInMs) {
        this.raccoonConfig = this.raccoonConfig.copyAndSetPeerMaxIdleTimeInMs(peerMaxIdleTimeInMs);
        return this;
    }

    /**
     * Sets the timeout for requests in the grid
     *
     * @param requestTimeoutInMs timeout in milliseconds
     * @return the builder object
     */
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

    public StorageGridBuilder withExecutors(StorageGridExecutors executors) {
        this.storageGridExecutors = executors;
        return this;
    }

    StorageGridBuilder() {

    }

    public StorageGrid build() {
        var raccoon = this.raccoonBuilder
                .withConfig(this.raccoonConfig)
                .build();
        var result = new StorageGrid(
                this.storageGridConfig,
                this.storageGridExecutors,
                raccoon,
                this.gridOpSerDe,
                this.context
        );
        return result;
    }


}
