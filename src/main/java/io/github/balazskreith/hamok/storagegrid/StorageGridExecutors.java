package io.github.balazskreith.hamok.storagegrid;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public interface StorageGridExecutors {
    ExecutorService getSyncOperationExecutor();
    ExecutorService getSendingMessageExecutor();
    ExecutorService getSubmissionExecutor();

    static StorageGridExecutors createDefault() {
        var sendingMessageExecutor = Executors.newSingleThreadExecutor();
        var defaultExecutor = Executors.newSingleThreadExecutor();
        return new StorageGridExecutors() {
            @Override
            public ExecutorService getSyncOperationExecutor() {
                return defaultExecutor;
            }

            @Override
            public ExecutorService getSendingMessageExecutor() {
                return sendingMessageExecutor;
            }

            @Override
            public ExecutorService getSubmissionExecutor() {
                return defaultExecutor;
            }
        };
    }
}
