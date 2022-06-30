package io.github.balazskreith.hamok;

import io.github.balazskreith.hamok.rxutils.RxCollector;
import io.github.balazskreith.hamok.rxutils.TimeoutController;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;

import java.util.List;
import java.util.function.Supplier;

public interface StorageEvents<K, V> extends Disposable {

    Observable<ModifiedStorageEntry<K, V>> createdEntry();
    Observable<ModifiedStorageEntry<K, V>> updatedEntry();
    Observable<ModifiedStorageEntry<K, V>> deletedEntry();
    Observable<ModifiedStorageEntry<K, V>> expiredEntry();
    Observable<ModifiedStorageEntry<K, V>> evictedEntry();

    Observable<String> closingStorage();

    default StorageEvents<K, V> observeOn(Supplier<Scheduler> schedulerSupplier) {
        var createdEntry = PublishSubject.<ModifiedStorageEntry<K, V>>create();
        var updatedEntry = PublishSubject.<ModifiedStorageEntry<K, V>>create();
        var deletedEntry = PublishSubject.<ModifiedStorageEntry<K, V>>create();
        var expiredEntry = PublishSubject.<ModifiedStorageEntry<K, V>>create();
        var evictedEntry = PublishSubject.<ModifiedStorageEntry<K, V>>create();
        var closingStorage = PublishSubject.<String>create();

        this.createdEntry().observeOn(schedulerSupplier.get()).subscribe(createdEntry);
        this.updatedEntry().observeOn(schedulerSupplier.get()).subscribe(updatedEntry);
        this.deletedEntry().observeOn(schedulerSupplier.get()).subscribe(deletedEntry);
        this.expiredEntry().observeOn(schedulerSupplier.get()).subscribe(expiredEntry);
        this.evictedEntry().observeOn(schedulerSupplier.get()).subscribe(evictedEntry);
        this.closingStorage().observeOn(schedulerSupplier.get()).subscribe(closingStorage);

        return new StorageEvents<K, V>() {
            @Override
            public Observable<ModifiedStorageEntry<K, V>> createdEntry() {
                return createdEntry;
            }

            @Override
            public Observable<ModifiedStorageEntry<K, V>> updatedEntry() {
                return updatedEntry;
            }

            @Override
            public Observable<ModifiedStorageEntry<K, V>> deletedEntry() {
                return deletedEntry;
            }

            @Override
            public Observable<ModifiedStorageEntry<K, V>> expiredEntry() {
                return expiredEntry;
            }

            @Override
            public Observable<ModifiedStorageEntry<K, V>> evictedEntry() {
                return evictedEntry;
            }

            @Override
            public Observable<String> closingStorage() {
                return closingStorage;
            }

            @Override
            public void dispose() {
                List.of(createdEntry, updatedEntry, deletedEntry, expiredEntry, evictedEntry).stream()
                        .filter(s -> !s.hasComplete() && !s.hasThrowable())
                        .forEach(Subject::onComplete);
            }

            @Override
            public boolean isDisposed() {
                return List.of(createdEntry, updatedEntry, deletedEntry, expiredEntry, evictedEntry).stream().allMatch(s -> s.hasComplete() || s.hasThrowable());
            }
        };
    }

    default CollectedStorageEvents<K, V> collectOn(Scheduler scheduler, int maxTimeInMs, int maxItems) {
        var timeoutController = new TimeoutController(maxTimeInMs, scheduler);
        var createdEntries = timeoutController.<ModifiedStorageEntry<K, V>>rxCollectorBuilder().withMaxItems(maxItems).build();
        var updatedEntries = timeoutController.<ModifiedStorageEntry<K, V>>rxCollectorBuilder().withMaxItems(maxItems).build();
        var deletedEntries = timeoutController.<ModifiedStorageEntry<K, V>>rxCollectorBuilder().withMaxItems(maxItems).build();
        var expiredEntries = timeoutController.<ModifiedStorageEntry<K, V>>rxCollectorBuilder().withMaxItems(maxItems).build();
        var evictedEntries = timeoutController.<ModifiedStorageEntry<K, V>>rxCollectorBuilder().withMaxItems(maxItems).build();
        var closingStorage = PublishSubject.<String>create();

        this.createdEntry().subscribe(createdEntries);
        this.updatedEntry().subscribe(updatedEntries);
        this.deletedEntry().subscribe(deletedEntries);
        this.expiredEntry().subscribe(expiredEntries);
        this.evictedEntry().subscribe(evictedEntries);
        this.closingStorage().observeOn(scheduler).subscribe(closingStorage);
        return new CollectedStorageEvents<K, V>() {
            @Override
            public Observable<List<ModifiedStorageEntry<K, V>>> createdEntries() {
                return createdEntries;
            }

            @Override
            public Observable<List<ModifiedStorageEntry<K, V>>> updatedEntries() {
                return updatedEntries;
            }

            @Override
            public Observable<List<ModifiedStorageEntry<K, V>>> deletedEntries() {
                return deletedEntries;
            }

            @Override
            public Observable<List<ModifiedStorageEntry<K, V>>> expiredEntries() {
                return expiredEntries;
            }

            @Override
            public Observable<List<ModifiedStorageEntry<K, V>>> evictedEntries() {
                return evictedEntries;
            }

            @Override
            public Observable<String> closingStorage() {
                return closingStorage;
            }

            @Override
            public void dispose() {
                if (this.isDisposed()) {
                    return;
                }
                List.of(createdEntries, updatedEntries, deletedEntries, expiredEntries, evictedEntries)
                        .stream().filter(c -> !c.isTerminated())
                        .forEach(RxCollector::onComplete);
            }

            @Override
            public boolean isDisposed() {
                return createdEntries.isTerminated() &&
                        updatedEntries.isTerminated() &&
                        deletedEntries.isTerminated() &&
                        expiredEntries.isTerminated() &&
                        evictedEntries.isTerminated();
            }
        };
    }
}
