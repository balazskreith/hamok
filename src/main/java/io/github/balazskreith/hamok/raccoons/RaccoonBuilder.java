package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.rxutils.RxTimeLimitedMap;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;

public class RaccoonBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Raccoon.class);

    private RaccoonConfig config = RaccoonConfig.create();
    private Scheduler scheduler = Schedulers.computation();
    private Map<Integer, LogEntry> providedLogEntryMap;
    private int logExpirationTimeInMs = 10000;
    public RaccoonBuilder withConfig(RaccoonConfig config) {
        this.config = config;
        return this;
    }

    public RaccoonBuilder withExecutor(Executor executor) {
        if (executor == null) return this;
        this.scheduler = Schedulers.from(executor);
        return this;
    }

    public RaccoonBuilder withScheduler(Scheduler scheduler) {
        if (scheduler == null) return this;
        this.scheduler = scheduler;
        return this;
    }

    public RaccoonBuilder withLogsExpirationTimeInMs(Integer expirationTimeInMs) {
        this.logExpirationTimeInMs = expirationTimeInMs;
        return this;
    }

    public RaccoonBuilder withLogsMap(Map<Integer, LogEntry> map) {
        if (map == null) return this;
        this.logExpirationTimeInMs = 0;
        this.providedLogEntryMap = map;
        return this;
    }

    public Raccoon build() {
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
            var result = new Raccoon(
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
