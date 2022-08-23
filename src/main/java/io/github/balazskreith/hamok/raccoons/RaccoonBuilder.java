package io.github.balazskreith.hamok.raccoons;

import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class RaccoonBuilder {
    private static final Logger logger = LoggerFactory.getLogger(Raccoon.class);

    private RaccoonConfig config = RaccoonConfig.create();
    private Scheduler scheduler = Schedulers.from(Executors.newSingleThreadExecutor());
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
                this.providedLogEntryMap = new HashMap<Integer, LogEntry>();
            }
            logs = new RaftLogs(this.providedLogEntryMap, this.logExpirationTimeInMs);
            var result = new Raccoon(
                    logs,
                    this.config,
                    this.scheduler,
                    disposer
            );
            disposer.add(result.committedEntries().subscribe(logEntry -> {
                if (result.syncProperties.lastApplied.get() < logEntry.index()) {
                    result.syncProperties.lastApplied.set(logEntry.index());
                }
            }));
            return result;
        } catch (Exception ex) {
            disposer.dispose();
            throw ex;
        }

    }

}
