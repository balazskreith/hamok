package io.github.balazskreith.vstorage;

import io.github.balazskreith.vstorage.rxutils.TimeoutController;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;

import java.util.List;
import java.util.function.Supplier;

public interface CollectedStorageEvents<K, V> extends Disposable {

    Observable<List<ModifiedStorageEntry<K, V>>> createdEntries();
    Observable<List<ModifiedStorageEntry<K, V>>> updatedEntries();
    Observable<List<ModifiedStorageEntry<K, V>>> deletedEntries();
    Observable<List<ModifiedStorageEntry<K, V>>> expiredEntries();
    Observable<List<ModifiedStorageEntry<K, V>>> evictedEntries();
    Observable<String> closingStorage();

    default CollectedStorageEvents<K, V> observeOn(Supplier<Scheduler> schedulerSupplier) {
        var createdEntries = PublishSubject.<List<ModifiedStorageEntry<K, V>>>create();
        var updatedEntries = PublishSubject.<List<ModifiedStorageEntry<K, V>>>create();
        var deletedEntries = PublishSubject.<List<ModifiedStorageEntry<K, V>>>create();
        var expiredEntries = PublishSubject.<List<ModifiedStorageEntry<K, V>>>create();
        var evictedEntries = PublishSubject.<List<ModifiedStorageEntry<K, V>>>create();
        var closingStorage = PublishSubject.<String>create();

        this.createdEntries().observeOn(schedulerSupplier.get()).subscribe(createdEntries);
        this.updatedEntries().observeOn(schedulerSupplier.get()).subscribe(updatedEntries);
        this.deletedEntries().observeOn(schedulerSupplier.get()).subscribe(deletedEntries);
        this.expiredEntries().observeOn(schedulerSupplier.get()).subscribe(expiredEntries);
        this.evictedEntries().observeOn(schedulerSupplier.get()).subscribe(evictedEntries);
        this.closingStorage().observeOn(schedulerSupplier.get()).subscribe(closingStorage);

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
                List.of(createdEntries,
                                updatedEntries,
                                deletedEntries,
                                expiredEntries,
                                evictedEntries,
                                closingStorage
                                ).stream()
                        .filter(s -> !s.hasComplete() && !s.hasThrowable())
                        .forEach(Subject::onComplete);
            }

            @Override
            public boolean isDisposed() {
                return List.of(createdEntries,
                        updatedEntries,
                        deletedEntries,
                        expiredEntries,
                        evictedEntries,
                        closingStorage).stream().allMatch(s -> s.hasComplete() || s.hasThrowable());
            }
        };
    }

    default StorageEvents<K, V> emitOn(Scheduler scheduler, int maxTimeInMs, int maxItems) {
        var timeoutController = new TimeoutController(maxTimeInMs, scheduler);
        var createdEntry = timeoutController.<ModifiedStorageEntry<K, V>>rxEmitterBuilder().withMaxItems(maxItems).build();
        var updatedEntry = timeoutController.<ModifiedStorageEntry<K, V>>rxEmitterBuilder().withMaxItems(maxItems).build();
        var deletedEntry = timeoutController.<ModifiedStorageEntry<K, V>>rxEmitterBuilder().withMaxItems(maxItems).build();
        var expiredEntry = timeoutController.<ModifiedStorageEntry<K, V>>rxEmitterBuilder().withMaxItems(maxItems).build();
        var evictedEntry = timeoutController.<ModifiedStorageEntry<K, V>>rxEmitterBuilder().withMaxItems(maxItems).build();

        var closingStorage = PublishSubject.<String>create();

        this.createdEntries().subscribe(createdEntry);
        this.updatedEntries().subscribe(updatedEntry);
        this.deletedEntries().subscribe(deletedEntry);
        this.expiredEntries().subscribe(expiredEntry);
        this.evictedEntries().subscribe(evictedEntry);
        this.closingStorage().subscribe(closingStorage);

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

            }

            @Override
            public boolean isDisposed() {
                return false;
            }
        };
    }
}
