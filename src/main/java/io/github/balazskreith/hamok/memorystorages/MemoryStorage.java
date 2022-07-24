package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.*;

import java.util.*;
import java.util.stream.Collectors;

public class MemoryStorage<K, V> implements Storage<K, V> {

	public static<U, R> MemoryStorageBuilder<U, R> builder() {
		return new MemoryStorageBuilder<U, R>();
	}

	private final Map<K, V> map = new HashMap<>();
	private final StorageEventDispatcher<K, V> eventDispatcher = new StorageEventDispatcher<>();
	private final String id;

	MemoryStorage() {
		this(UUID.randomUUID().toString());
	}

	MemoryStorage(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public int size() {
		return this.map.size();
	}

	@Override
	public void clear() {
		var events = this.map.entrySet()
				.stream()
				.map(entry -> StorageEvent.makeEvictedEntryEvent(this.id, entry.getKey(), entry.getValue()))
				.collect(Collectors.toList());
		this.map.clear();
		events.forEach(this.eventDispatcher::accept);
	}

	@Override
	public Set<K> keys() {
		return this.map.keySet();
	}

	public StorageEvents<K, V> events() {
		return this.eventDispatcher;
	}

	@Override
	public boolean isEmpty() {
		return this.map.isEmpty();
	}

	@Override
	public V get(K key) {
		return this.map.get(key);
	}

	@Override
	public Map<K, V> getAll(Set<K> keys) {
		var result = new HashMap<K, V>();
		for ( var key : keys) {
			var value = this.map.get(key);
			if (value == null) continue;
			result.put(key, value);
		}
		return Collections.unmodifiableMap(result);
	}


	@Override
	public V set(K key, V value) {
		var oldValue = this.map.put(key, value);
		StorageEvent<K, V> event;
		if (oldValue == null) {
			event = StorageEvent.makeCreatedEntryEvent(this.id, key, value);
		} else {
			event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, value);
		}
		this.eventDispatcher.accept(event);
		return oldValue;
	}

	@Override
	public Map<K, V> setAll(Map<K, V> map) {
		var keys = map.keySet();
		var oldEntries = this.getAll(keys);
		var events = new LinkedList<StorageEvent<K, V>>();
		this.map.putAll(map);
		for (var key : keys) {
			var oldValue = oldEntries.get(key);
			var newValue = this.map.get(key);
			StorageEvent<K, V> event;
			if (oldValue == null) {
				event = StorageEvent.makeCreatedEntryEvent(this.id, key, newValue);
			} else {
				event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
			}
			events.add(event);
		}
		try {
			return oldEntries;
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}

	}

	@Override
	public boolean delete(K key) {
		var oldValue = this.map.remove(key);
		if (oldValue == null) {
			return false;
		}
		try {
			return true;
		} finally {
			var event = StorageEvent.makeDeletedEntryEvent(this.id, key, oldValue);
			this.eventDispatcher.accept(event);
		}
	}

	@Override
	public Set<K> deleteAll(Set<K> keys) {
		return keys.stream().filter(this::delete).collect(Collectors.toSet());
	}


	@Override
	public Iterator<StorageEntry<K, V>> iterator() {
		var iterator = this.map.entrySet().iterator();
		return new Iterator<StorageEntry<K, V>>() {
			@Override
			public boolean hasNext() {
				return iterator.hasNext();
			}

			@Override
			public StorageEntry<K, V> next() {
				var entry = iterator.next();
				return StorageEntry.create(entry.getKey(), entry.getValue());
			}
		};
	}

	@Override
	public void close() throws Exception {
		if (!this.eventDispatcher.isDisposed()) {
			this.eventDispatcher.accept(StorageEvent.makeClosingStorageEvent(this.id));
			this.eventDispatcher.dispose();
		}
		this.map.clear();
	}

	@Override
	public Map<K, V> insertAll(Map<K, V> entries) {
		if (entries == null || entries.size() < 1) return Collections.emptyMap();
		var result = new HashMap<K, V>();
		var events = new LinkedList<StorageEvent<K, V>>();
		var it = entries.entrySet().iterator();
		for (; it.hasNext(); ) {
			var entry = it.next();
			var key = entry.getKey();
			var oldValue = this.map.putIfAbsent(entry.getKey(), entry.getValue());
			if (oldValue != null) {
				result.put(key, oldValue);
			} else {
				var event = StorageEvent.makeCreatedEntryEvent(this.id, key, entry.getValue());
				events.add(event);
			}
		}
		try {
			return result;
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}

	}
}
