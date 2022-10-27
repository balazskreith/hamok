package io.github.balazskreith.hamok.storagegrid;

public interface StorageInGridMetrics {
    int getPendingRequests();
    int getPendingResponses();
    int failedRequests();
}
