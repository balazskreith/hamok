package io.github.balazskreith.hamok.storagegrid;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public abstract class StorageGridMetrics implements Supplier<StorageGridStats> {

    private volatile int replicatedStorages = 0;
    private volatile int separatedStorages = 0;
    private volatile long sentBytes = 0;
    private volatile long receivedBytes = 0;
    private volatile int sentMessages = 0;
    private volatile int receivedMessages = 0;

    private AtomicReference<Snapshot> snapshot = new AtomicReference(null);

    void incrementReplicatedStorage() {
        ++this.replicatedStorages;
    }

    void incrementSeparatedStorage() {
        ++this.separatedStorages;
    }

    void incrementSentBytes(long value) {
        this.sentBytes += value;
    }

    void incrementReceivedBytes(long value) {
        this.receivedBytes += value;
    }

    void incrementSentMessages() {
        ++this.sentMessages;
    }

    void incrementReceivedMessages() {
        ++this.receivedMessages;
    }


    void decrementReplicatedStorage() {
        --this.replicatedStorages;
    }

    void decrementSeparatedStorage() {
        --this.separatedStorages;
    }

    protected abstract int pendingRequests();
    protected abstract int pendingResponses();

    public StorageGridStats get() {
        var snapshot = this.snapshot.get();
        if (snapshot == null) {
            snapshot = this.createSnapshot();
            this.snapshot.set(snapshot);
        }
        var now = Instant.now().toEpochMilli();
        if (now - 5000 < snapshot.created) {
            return snapshot.stats;
        }
        snapshot = this.createSnapshot();
        this.snapshot.set(snapshot);
        return snapshot.stats;
    }

    private Snapshot createSnapshot() {
        var created = Instant.now().toEpochMilli();
        var stats = new StorageGridStats(
                this.replicatedStorages,
                this.separatedStorages,
                this.sentBytes,
                this.receivedBytes,
                this.sentMessages,
                this.receivedMessages,
                this.pendingRequests(),
                this.pendingResponses()
        );
        return new Snapshot(
                created,
                stats
        );
    }

    private record Snapshot(
            long created,
            StorageGridStats stats
    ) {

    }
}
