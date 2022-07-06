package io.github.balazskreith.hamok.racoon;

import io.github.balazskreith.hamok.rxutils.RxTimeLimitedMap;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Executor;

public class RacoonBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Racoon.class);

    private RacoonConfig config = new RacoonConfig(
            UUID.randomUUID(), // the local peer id
            1000, // election timeout
            1000, // timeout for followers before starts an election
            300, // heartbeat
            1000, // hello message sending if auto discovery on
            1500, // the max idle time for a follower to be idle before the leader renders it inactive
            10000, // the timeout for waiting an application orchestrated commit sync
            true // indicate if remote peer discovery should be automatic or manual
    );
    private Scheduler scheduler = Schedulers.computation();
    private Map<Integer, LogEntry> providedLogEntryMap;
    private int logExpirationTimeInMs = 10000;
    public RacoonBuilder withConfig(RacoonConfig config) {
        this.config = config;
        return this;
    }

    public RacoonBuilder withExecutor(Executor executor) {
        if (executor == null) return this;
        this.scheduler = Schedulers.from(executor);
        return this;
    }

    public RacoonBuilder withScheduler(Scheduler scheduler) {
        if (scheduler == null) return this;
        this.scheduler = scheduler;
        return this;
    }

    public RacoonBuilder withLogsExpirationTimeInMs(Integer expirationTimeInMs) {
        this.logExpirationTimeInMs = expirationTimeInMs;
        return this;
    }

    public RacoonBuilder withLogsMap(Map<Integer, LogEntry> map) {
        if (map == null) return this;
        this.logExpirationTimeInMs = 0;
        this.providedLogEntryMap = map;
        return this;
    }

    public Racoon build() {
        Objects.requireNonNull(this.config, "Raft cannot be build without config");
        var disposer = new CompositeDisposable();
        try {
            RaftLogs logs;
            if (this.providedLogEntryMap == null) {
                logger.info("Raft uses time limited map for logs. Expiration time is {} ms", this.logExpirationTimeInMs);
                var map = new RxTimeLimitedMap<Integer, LogEntry>(this.logExpirationTimeInMs);
                logs = new RaftLogs(map);

                // this is crucial here to feed back the map on a different thread from its emission.
                // otherwise, we can cause a deadlock
                var disposable = map.expiredEntry().observeOn(Schedulers.computation()).subscribe(entry -> {
                    logs.expire(entry.getKey());
                });
                disposer.add(disposable);
            } else {
                if (0 < this.logExpirationTimeInMs) {
                    logger.warn("Building with custom map makes the provided building attribute logExpiration time ineffective. If expiration of entry is needed it need to be inside the provided map.");
                }
                logs = new RaftLogs(this.providedLogEntryMap);
            }
            var result = new Racoon(
                    logs,
                    this.config,
                    this.scheduler,
                    disposer
            );
            disposer.add(logs.committedEntries().subscribe(logEntry -> {
                result.syncProperties.lastApplied.set(logEntry.index());
            }));
            return result;
        } catch (Exception ex) {
            disposer.dispose();
            throw ex;
        }

    }
}
