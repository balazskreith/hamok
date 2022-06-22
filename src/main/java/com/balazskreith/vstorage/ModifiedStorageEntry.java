package com.balazskreith.vstorage;

import java.time.Instant;
import java.util.Objects;

public interface ModifiedStorageEntry<K, V> {

    static<U, R> ModifiedStorageEntry<U, R> create(U key, R oldValue, R newValue) {
        Objects.requireNonNull(key, "Key must not be null");
        Long timestamp = Instant.now().toEpochMilli();
        return new ModifiedStorageEntry<U, R>() {
            @Override
            public U getKey() { return key; }

            @Override
            public Long getTimestamp() {
                return timestamp;
            }

            @Override
            public R getOldValue() {
                return oldValue;
            }

            @Override
            public R getNewValue() {
                return newValue;
            }

            @Override
            public int hashCode() {
                return key.hashCode() ^ oldValue.hashCode() ^ newValue.hashCode();
            }

            @Override
            public boolean equals(Object o) {
                if (o == this) return true;
                if (o instanceof ModifiedStorageEntry<?,?> == false) return false;
                var other = (ModifiedStorageEntry<U, R>) o;
                if (!key.equals(other.getKey())) return false;
                if (oldValue == null) {
                    if (other.getOldValue() != null) return false;
                } else if (!oldValue.equals(other.getOldValue())) {
                    return false;
                }
                if (newValue == null) {
                    if (other.getNewValue() != null) return false;
                } else if (!newValue.equals(other.getNewValue())) {
                    return false;
                }
                return true;
            }
        };
    }

    K getKey();

    V getOldValue();

    V getNewValue();

    Long getTimestamp();
}
