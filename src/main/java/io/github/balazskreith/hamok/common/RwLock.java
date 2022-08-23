package io.github.balazskreith.hamok.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class RwLock {
    private static final Logger logger = LoggerFactory.getLogger(RwLock.class);

    private final String context;
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public RwLock() {
        this(null);
    }

    public RwLock(String context) {
        this.context = context;
    }

    public void runInWriteLock(Runnable action) {
        var writeMutex = this.rwLock.writeLock();
        try {
            if (this.context != null) {
                logger.info("Begin {} runInWriteLock", this.context);
            }
            writeMutex.lock();
            action.run();
        } finally {
            writeMutex.unlock();
            if (this.context != null) {
                logger.info("End {} runInWriteLock", this.context);
            }
        }
    }

    public void runInReadLock(Runnable action) {
        var readLock = this.rwLock.readLock();
        try {
            if (this.context != null) {
                logger.info("Begin {} runInReadLock", this.context);
            }
            readLock.lock();
            action.run();
        } finally {
            readLock.unlock();
            if (this.context != null) {
                logger.info("End {} runInReadLock", this.context);
            }
        }
    }

    public<U> U supplyInWriteLock(Supplier<U> supplier) {
        var writeMutex = this.rwLock.writeLock();
        try {
            if (this.context != null) {
                logger.info("Begin {} supplyInWriteLock", this.context);
            }
            writeMutex.lock();
            return supplier.get();
        } finally {
            writeMutex.unlock();
            if (this.context != null) {
                logger.info("End {} supplyInWriteLock", this.context);
            }
        }
    }

    public<U> U supplyInReadLock(Supplier<U> supplier) {
        var readMutex = this.rwLock.readLock();
        try {
            if (this.context != null) {
                logger.info("Begin {} supplyInReadLock", this.context);
            }
            readMutex.lock();
            return supplier.get();
        } finally {
            readMutex.unlock();
            if (this.context != null) {
                logger.info("End {} supplyInReadLock", this.context);
            }
        }
    }
}
