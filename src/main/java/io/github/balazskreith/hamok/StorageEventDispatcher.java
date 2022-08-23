package io.github.balazskreith.hamok;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;

import java.util.List;
import java.util.function.Consumer;

public class StorageEventDispatcher<K, V> implements StorageEvents<K, V>, Consumer<StorageEvent<K, V>> {
    private final Subject<ModifiedStorageEntry<K, V>> createdEntrySubject = PublishSubject.create();
    private final Subject<ModifiedStorageEntry<K, V>> updatedEntrySubject = PublishSubject.create();
    private final Subject<ModifiedStorageEntry<K, V>> deletedEntrySubject = PublishSubject.create();
    private final Subject<ModifiedStorageEntry<K, V>> expiredEntrySubject = PublishSubject.create();
    private final Subject<ModifiedStorageEntry<K, V>> evictedEntrySubject = PublishSubject.create();
    private final Subject<ModifiedStorageEntry<K, V>> restoredEntrySubject = PublishSubject.create();
    private final Subject<String> closingStorageSubject = PublishSubject.create();

    public StorageEventDispatcher() {

    }

    public void accept(StorageEvent<K, V> storageEntryEvent) {
        var modifiedStorageEntry = storageEntryEvent.getModifiedStorageEntry();
        switch (storageEntryEvent.getEventType()) {
            case CREATED_ENTRY -> this.createdEntrySubject.onNext(modifiedStorageEntry);
            case UPDATED_ENTRY -> this.updatedEntrySubject.onNext(modifiedStorageEntry);
            case DELETED_ENTRY -> this.deletedEntrySubject.onNext(modifiedStorageEntry);
            case EXPIRED_ENTRY -> this.expiredEntrySubject.onNext(modifiedStorageEntry);
            case EVICTED_ENTRY -> this.evictedEntrySubject.onNext(modifiedStorageEntry);
            case RESTORED_ENTRY -> this.restoredEntrySubject.onNext(modifiedStorageEntry);
            case CLOSING_STORAGE -> this.closingStorageSubject.onNext(storageEntryEvent.getStorageId());
        }
    }

    @Override
    public Observable<ModifiedStorageEntry<K, V>> createdEntry() {
        return this.createdEntrySubject;
    }

    @Override
    public Observable<ModifiedStorageEntry<K, V>> updatedEntry() {
        return this.updatedEntrySubject;
    }

    @Override
    public Observable<ModifiedStorageEntry<K, V>> deletedEntry() {
        return this.deletedEntrySubject;
    }

    @Override
    public Observable<ModifiedStorageEntry<K, V>> expiredEntry() {
        return this.expiredEntrySubject;
    }

    @Override
    public Observable<ModifiedStorageEntry<K, V>> evictedEntry() {
        return this.evictedEntrySubject;
    }

    @Override
    public Observable<ModifiedStorageEntry<K, V>> restoredEntry() {
        return this.restoredEntrySubject;
    }

    @Override
    public Observable<String> closingStorage() { return this.closingStorageSubject; }

    @Override
    public void dispose() {
        if (this.isDisposed()) {
            return;
        }
        List.of(this.createdEntrySubject, this.updatedEntrySubject, this.deletedEntrySubject, this.expiredEntrySubject, this.evictedEntrySubject).stream()
                .filter(Subject::hasComplete)
                .forEach(Subject::onComplete);
    }

    @Override
    public boolean isDisposed() {
        return (this.createdEntrySubject.hasComplete() || this.createdEntrySubject.hasThrowable()) &&
                (this.updatedEntrySubject.hasComplete() || this.updatedEntrySubject.hasThrowable()) &&
                (this.deletedEntrySubject.hasComplete() || this.deletedEntrySubject.hasThrowable()) &&
                (this.expiredEntrySubject.hasComplete() || this.expiredEntrySubject.hasThrowable()) &&
                (this.evictedEntrySubject.hasComplete() || this.evictedEntrySubject.hasThrowable()) &&
                (this.restoredEntrySubject.hasComplete() || this.restoredEntrySubject.hasThrowable()) &&
                true
                ;
    }


}
