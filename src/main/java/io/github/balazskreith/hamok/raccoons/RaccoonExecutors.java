package io.github.balazskreith.hamok.raccoons;

import java.util.concurrent.ExecutorService;

public interface RaccoonExecutors {
    ExecutorService getDiscoveryExecutor();
    ExecutorService getRunStateExecutor();
}
