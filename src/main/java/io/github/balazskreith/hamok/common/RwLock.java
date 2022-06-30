package io.github.balazskreith.hamok.common;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class RwLock {
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public void runInWriteLock(Runnable action) {
        var writeMutex = this.rwLock.writeLock();
        try {
            writeMutex.lock();
            action.run();
        } finally {
            writeMutex.unlock();
        }
    }

    public void runInReadLock(Runnable action) {
        var readLock = this.rwLock.readLock();
        try {
            readLock.lock();
            action.run();
        } finally {
            readLock.unlock();
        }
    }

    public<U> U supplyInWriteLock(Supplier<U> supplier) {
        var writeMutex = this.rwLock.writeLock();
        try {
            writeMutex.lock();
            return supplier.get();
        } finally {
            writeMutex.unlock();
        }
    }

    public<U> U supplyInReadLock(Supplier<U> supplier) {
        var readMutex = this.rwLock.readLock();
        try {
            readMutex.lock();
            return supplier.get();
        } finally {
            readMutex.unlock();
        }
    }
}
