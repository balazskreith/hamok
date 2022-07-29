package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.StorageFromMap;

import java.util.HashMap;
import java.util.UUID;

public class MemoryStorage<K, V> extends StorageFromMap<K, V> {

	public static<U, R> MemoryStorageBuilder<U, R> builder() {
		return new MemoryStorageBuilder<U, R>();
	}

	MemoryStorage() {
		this(UUID.randomUUID().toString());
	}

	MemoryStorage(String id) {
		super(id, new HashMap<K, V>());
	}
}
