package io.github.balazskreith.hamok.raccoons;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SyncedProperties {

    /* Persistent state on all servers: */
    /**
     * latest term server has seen (initialized to 0
     * on first boot, increases monotonically)
     */
    final AtomicInteger currentTerm = new AtomicInteger(0);

    /* Volatile state on all servers: */
    /**
     * candidateId that received vote in current
     * term (or null if none)
     */
    final AtomicReference<UUID> votedFor = new AtomicReference<>(null);


    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    final AtomicInteger lastApplied = new AtomicInteger(-1);

    /* Volatile state on leaders (Reinitialized after election): */
    /**
     * for each server, index of the next log entry
     * to send to that server (initialized to leader
     * last log index + 1)
     */
    final Map<UUID, Integer> nextIndex = new ConcurrentHashMap<>();

    /**
     * for each server, index of highest log entry
     * known to be replicated on server
     * (initialized to 0, increases monotonically)
     */
    final Map<UUID, Integer> matchIndex = new ConcurrentHashMap<>();
}
