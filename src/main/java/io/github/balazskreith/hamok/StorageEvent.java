package io.github.balazskreith.hamok;

public interface StorageEvent<K, V> {

    static<U, R> StorageEvent<U, R> createEvent(String storageId, StorageEventTypes type, ModifiedStorageEntry<U, R> modifiedStorageEntry) {
        return new StorageEvent<U, R>() {
            @Override
            public String getStorageId() {
                return storageId;
            }

            @Override
            public StorageEventTypes getEventType() {
                return type;
            }

            @Override
            public ModifiedStorageEntry<U, R> getModifiedStorageEntry() {
                return modifiedStorageEntry;
            }
        };
    }

    static<U, R> StorageEvent<U, R> makeCreatedEntryEvent(String storageId, U key, R value) {
        var modifiedStorageEntry = ModifiedStorageEntry.create(key, null, value);
        return createEvent(storageId, StorageEventTypes.CREATED_ENTRY, modifiedStorageEntry);
    }

    static<U, R> StorageEvent<U, R> makeUpdatedEntryEvent(String storageId, U key, R oldValue, R newValue) {
        var modifiedStorageEntry = ModifiedStorageEntry.create(key, oldValue, newValue);
        return createEvent(storageId, StorageEventTypes.UPDATED_ENTRY, modifiedStorageEntry);
    }

    static<U, R> StorageEvent<U, R> makeDeletedEntryEvent(String storageId, U key, R value) {
        var modifiedStorageEntry = ModifiedStorageEntry.create(key, value, null);
        return createEvent(storageId, StorageEventTypes.DELETED_ENTRY, modifiedStorageEntry);
    }

    static<U, R> StorageEvent<U, R> makeExpiredEntryEvent(String storageId, U key, R value) {
        var modifiedStorageEntry = ModifiedStorageEntry.create(key, value, null);
        return createEvent(storageId, StorageEventTypes.EXPIRED_ENTRY, modifiedStorageEntry);
    }

    static<U, R> StorageEvent<U, R> makeEvictedEntryEvent(String storageId, U key, R value) {
        var modifiedStorageEntry = ModifiedStorageEntry.create(key, value, null);
        return createEvent(storageId, StorageEventTypes.EVICTED_ENTRY, modifiedStorageEntry);
    }

    static<U, R> StorageEvent<U, R> makeRestoredEntryEvent(String storageId, U key, R value) {
        var modifiedStorageEntry = ModifiedStorageEntry.create(key, null, value);
        return createEvent(storageId, StorageEventTypes.RESTORED_ENTRY, modifiedStorageEntry);
    }

    static<U, R> StorageEvent<U, R> makeClosingStorageEvent(String storageId) {
        return createEvent(storageId, StorageEventTypes.CLOSING_STORAGE, null);
    }

    String getStorageId();

    StorageEventTypes getEventType();

    ModifiedStorageEntry<K, V> getModifiedStorageEntry();
}
