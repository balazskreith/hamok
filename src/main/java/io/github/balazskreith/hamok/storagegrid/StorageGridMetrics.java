package io.github.balazskreith.hamok.storagegrid;

public class StorageGridMetrics {

    private volatile int propagatedCollections = 0;
    private volatile int replicatedStorages = 0;
    private volatile int separatedStorages = 0;
    private volatile long sentBytes = 0;
    private volatile long receivedBytes = 0;
    private volatile int sentMessages = 0;
    private volatile int receivedMessages = 0;

    void incrementPropagatedCollections() {
        ++this.propagatedCollections;
    }

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

    void decrementPropagatedCollections() {
        --this.propagatedCollections;
    }

    void decrementReplicatedStorage() {
        --this.replicatedStorages;
    }

    void decrementSeparatedStorage() {
        --this.separatedStorages;
    }


    public StorageGridStats makeSnapshot() {
        return new StorageGridStats(
                this.propagatedCollections,
                this.replicatedStorages,
                this.separatedStorages,
                this.sentBytes,
                this.receivedBytes,
                this.sentMessages,
                this.receivedMessages
        );
    }
}
