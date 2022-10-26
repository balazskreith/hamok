package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

class RaftLogs {
    private static final Logger logger = LoggerFactory.getLogger(RaftLogs.class);

    /**
     * index of highest log entry applied to state
     * machine (initialized to 0, increases
     * monotonically)
     */
    private int lastApplied;

    /**
     * The next log index
     */
    private int nextIndex;

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    private int commitIndex;

    private final RwLock rwLock = new RwLock();
    private final Map<Integer, LogEntry> entries;
    private final int expirationTimeInMs;

    RaftLogs(Map<Integer, LogEntry> entries, int expirationTimeInMs) {
        this.entries = entries;
        this.expirationTimeInMs = expirationTimeInMs;
        this.commitIndex = -1;
        this.lastApplied = 0;
        this.nextIndex = 0;
    }

    /**
     * index of highest log entry known to be
     * committed (initialized to 0, increases
     * monotonically)
     */
    public int getCommitIndex() {
        return this.rwLock.supplyInReadLock(() -> this.commitIndex);
    }

    /**
     * The next index for the logs to be used if an entry is added or submitted
     * @return
     */
    public int getNextIndex() {
        return this.rwLock.supplyInReadLock(() -> {
            return this.nextIndex;
        });
    }

    public int getLastApplied() {
        return this.rwLock.supplyInReadLock(() -> {
            return this.lastApplied;
        });
    }

    public int size() {
        return this.rwLock.supplyInReadLock(this.entries::size);
    }

    private void expire() {
        if (this.expirationTimeInMs < 1) {
            // infinite
            return;
        }
        int expiredLogIndex = this.rwLock.supplyInReadLock(() -> {
            var thresholdInMs = Instant.now().toEpochMilli() - this.expirationTimeInMs;
            int result = -1;
            for (int index = this.lastApplied; index < this.commitIndex; ++index) {
                var logEntry = this.entries.get(index);
                if (logEntry == null) {
                    // already purged?
                    logger.info("LastApplied is set to {}, because for index {} logEntry does not exists", index + 1, index);
                    this.lastApplied = index + 1;
                    continue;
                }
                if (thresholdInMs <= logEntry.timestamp()) {
                    return result;
                }
                result = index;
            }
            return result;
        });
        if (expiredLogIndex < 0) {
            return;
        }
        this.rwLock.runInWriteLock(() -> {
            if (this.nextIndex <= expiredLogIndex || expiredLogIndex < this.lastApplied) {
                return;
            }
            if (this.commitIndex < expiredLogIndex) {
                logger.warn("expired log index is higher than the commit index. This is a problem! increase the expiration timeout, because it leads to a potential inconsistency issue.");
            }
            var removed = 0;
            for (int index = this.lastApplied; index < expiredLogIndex; ++index) {
                if (this.entries.remove(index) != null) {
                    ++removed;
                }
            }
            this.lastApplied = expiredLogIndex;
            logger.trace("Set the lastApplied to {} and removed {} items", this.lastApplied, removed);
        });
    }

    public List<LogEntry> commitUntil(int newCommitIndex) {
        var allowed = this.rwLock.supplyInReadLock(() -> {
           if (newCommitIndex <= this.commitIndex) {
               logger.warn("Requested to commit until {} index, but the actual commitIndex is {}", newCommitIndex, this.commitIndex);
               return false;
           }
           return true;
        });
        if (!allowed) {
            return Collections.emptyList();
        }
        var committedEntries = new LinkedList<LogEntry>();
        this.rwLock.runInWriteLock(() -> {
            while (this.commitIndex < newCommitIndex) {
                if (this.nextIndex <= this.commitIndex + 1) {
                    logger.warn("Cannot commit, because there is no next entry to commit. commitIndex: {}, nextIndex: {}, newCommitIndex: {}", this.commitIndex, this.nextIndex, newCommitIndex);
                    return;
                }
                var nextCommitIndex = this.commitIndex + 1;
                var logEntry = this.entries.get(nextCommitIndex);
                if (logEntry == null) {
                    logger.warn("LogEntry for nextCommitIndex {} is null. it supposed not to be null.", nextCommitIndex);
                    return;
                }
                this.commitIndex = nextCommitIndex;
                committedEntries.add(logEntry);
            }
        });
        if (0 < committedEntries.size()) {
            this.expire();
        }
        return committedEntries;
    }

//    public void commit() {
//        var result = this.rwLock.supplyInWriteLock(() -> {
//            if (this.nextIndex <= this.commitIndex + 1) {
//                logger.warn("Cannot commit index {}, because there is no next entry to commit. commitIndex: {}, nextIndex: {}", this.commitIndex, this.nextIndex);
//                return null;
//            }
//            var nextCommitIndex = this.commitIndex + 1;
//            var logEntry = this.entries.get(nextCommitIndex);
//            if (logEntry == null) {
//                logger.warn("LogEntry for nextCommitIndex {} is null. it supposed not to be null.", nextCommitIndex);
//                return null;
//            }
//            this.commitIndex = nextCommitIndex;
//            return logEntry;
//        });
//        if (result != null) {
//            this.committedEntries.onNext(result);
//        } else {
//            logger.warn("Nothing to commit");
//        }
//    }

    public Integer submit(int term, Message entry) {
        var now = Instant.now().toEpochMilli();
        return this.rwLock.supplyInWriteLock(() -> {
            var logEntry = new LogEntry(this.nextIndex, term, entry, now);
//            logger.info("Adding entry with index: {}", logEntry);
            this.entries.put(logEntry.index(), logEntry);
            ++this.nextIndex;
            return logEntry.index();
        });
    }

    /**
     * Compare the index and the term of the logEntry found in the logs
     * override if:
     *  - no entries found at the index
     *  - the log entry found in the index is different then the expected term
     *  Not override if:
     *   - the index is equal or higher than the logs nextIndex
     *   - the log index and term are equal
     * @param index
     * @param expectedTerm
     * @param entry
     * @return
     */
    public LogEntry compareAndOverride(int index, int expectedTerm, Message entry) throws IllegalAccessError{
        var now = Instant.now().toEpochMilli();
        return this.rwLock.supplyInWriteLock(() -> {
            if (this.nextIndex <= index) {
                return null;
            }
            var oldLogEntry = this.entries.get(index);
            if (oldLogEntry == null) {
                var newLogEntry = new LogEntry(index, expectedTerm, entry, now);
                this.entries.put(newLogEntry.index(), newLogEntry);
                return null;
            }
            if (expectedTerm == oldLogEntry.term()) {
                // theoretically identical
                return null;
            }
            var newLogEntry = new LogEntry(oldLogEntry.index(), expectedTerm, entry, now);
            this.entries.put(newLogEntry.index(), newLogEntry);
            return oldLogEntry;
        });
    }

    /**
     * Compare if the given index is indeed the next index and add the log if it does
     * @param expectedNextIndex the index expected to be the next
     * @param term the term the log is created
     * @param entry the entry of the log to be added
     * @return True if the expected index is the next index and the log is added, false otherwise
     */
    public boolean compareAndAdd(int expectedNextIndex, int term, Message entry) {
        var now = Instant.now().toEpochMilli();
        return this.rwLock.supplyInWriteLock(() -> {
            if (this.nextIndex != expectedNextIndex) {
                return false;
            }
            var logEntry = new LogEntry(this.nextIndex, term, entry, now);
            this.entries.put(logEntry.index(), logEntry);
            ++this.nextIndex;
            return true;
        });
    }

    public LogEntry get(int index) {
        return this.rwLock.supplyInReadLock(() -> {
            return this.entries.get(index);
        });
    }

    public List<Message> collectEntries(int startIndex) {
        List<Message> result = new ArrayList<>();
        return this.rwLock.supplyInReadLock(() -> {
            int missingEntries = 0;
            for (int logIndex = startIndex; logIndex < this.nextIndex; ++logIndex) {
                var logEntry = this.entries.get(logIndex);
                if (logEntry == null) {
                    // we don't have it anymore
                    ++missingEntries;
                    continue;
                }
                result.add(logEntry.entry());
            }
            if (0 < missingEntries) {
                logger.info("Requested to collect entries, startIndex: {}, endIndex: {}, but missing {} entries probably in the beginning. The other peer should request a commit sync", startIndex, this.nextIndex, missingEntries);
            }
//            if (0 < result.size()) {
//                logger.info("Retrieved 0 < result entries");
//            }
            return result;
        });
    }

    /**
     * Direct iterator for the logs. Starts with commitIndex + 1, and iterates the logs until nextIndex.
     * if a log is comitted after this iterator has been created but before the next is called on this iterator
     * the returned logEntry is null. if that is not desired use safeIterator
     * @return
     */
    public Iterator<LogEntry> iterator() {
        return new Iterator<LogEntry>() {
            private int index = RaftLogs.this.getCommitIndex() + 1;
            @Override
            public boolean hasNext() {
                return this.index < RaftLogs.this.getNextIndex();
            }

            @Override
            public LogEntry next() {
                return RaftLogs.this.get(this.index);
            }
        };
    }

    /**
     * thread safe iterator. the elements waiting to be committed are collected to a list before the iteration starts,
     * hence any commit or change of the logs not effecting the iteration
     * @return
     */
    public Iterator<LogEntry> safeIterator() {
        var startIndex = this.getCommitIndex() + 1;
        var endIndex = this.getNextIndex();
        var list = new LinkedList<LogEntry>();
        this.rwLock.runInReadLock(() -> {
            for (int index = startIndex; index < endIndex; ++index) {
                var logEntry = this.entries.get(index);
                list.add(logEntry);
            }
        });
        return list.iterator();
    }

    public void reset(int newCommitIndex) {
        this.rwLock.runInWriteLock(() -> {
            this.entries.clear();
            this.commitIndex = newCommitIndex;
            this.nextIndex = newCommitIndex + 1;
            this.lastApplied = newCommitIndex;
        });
        logger.info("Logs are reset. new values: commitIndex: {}, nextIndex: {}, lastApplied: {}", newCommitIndex, newCommitIndex + 1, newCommitIndex);
    }
}
