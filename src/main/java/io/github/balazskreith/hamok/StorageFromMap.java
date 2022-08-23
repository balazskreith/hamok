package io.github.balazskreith.hamok;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class StorageFromMap<K, V> implements Storage<K, V> {

	private final Map<K, V> map;
	private final StorageEventDispatcher<K, V> eventDispatcher = new StorageEventDispatcher<>();
	private final String id;
	private final BinaryOperator<V> mergeOp;

	public StorageFromMap(Map<K, V> map) {
		this(UUID.randomUUID().toString(), map);
	}

	public StorageFromMap(String id, Map<K, V> map) {
		this(UUID.randomUUID().toString(), map, (oldValue, newValue) -> newValue);
	}

	public StorageFromMap(String id, Map<K, V> map, BinaryOperator<V> mergeOp) {
		this.id = id;
		this.map = map;
		this.mergeOp = mergeOp;
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
		var oldValue = this.map.get(key);
		StorageEvent<K, V> event;
		if (oldValue == null) {
			this.map.put(key, value);
			event = StorageEvent.makeCreatedEntryEvent(this.id, key, value);
		} else {
			var newValue = this.mergeOp.apply(oldValue, value);
			this.map.put(key, newValue);
			event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
		}
		this.eventDispatcher.accept(event);
		return oldValue;
	}

	@Override
	public Map<K, V> setAll(Map<K, V> map) {
		var keys = map.keySet();
		var oldEntries = this.getAll(keys);
		var events = new LinkedList<StorageEvent<K, V>>();
		var inserts = new HashMap<K, V>();
		var updates = new HashMap<K, V>();
		for (var key : keys ) {
			var oldValue = this.map.get(key);
			var newValue = map.get(key);
			StorageEvent<K, V> event;
			if (oldValue == null) {
				event = StorageEvent.makeCreatedEntryEvent(this.id, key, map.get(key));
				inserts.put(key, newValue);
			} else {
				newValue = this.mergeOp.apply(oldValue, newValue);
				event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
				updates.put(key, newValue);
			}
			events.add(event);
		}
		if (0 < inserts.size()) {
			this.map.putAll(inserts);
		}
		if (0 < updates.size()) {
			this.map.putAll(updates);
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
	public void evict(K key) {
		var oldValue = this.map.remove(key);
		if (oldValue == null) {
			return;
		}
		var event = StorageEvent.makeEvictedEntryEvent(this.id, key, oldValue);
		this.eventDispatcher.accept(event);
	}

	@Override
	public void evictAll(Set<K> keys) {
		keys.stream().forEach(this::evict);
	}

	@Override
	public void restore(K key, V value) {
		if (this.map.containsKey(key)) {
			var message = String.format("Cannot restore already presented entries. key: %s, value: %s", key.toString(), value.toString());
			throw new FailedOperationException(message);
		}
		this.map.put(key, value);
		var event = StorageEvent.makeRestoredEntryEvent(this.id, key, value);
		this.eventDispatcher.accept(event);
	}

	@Override
	public void restoreAll(Map<K, V> entries) {
		var keys = entries.keySet();
		var events = new LinkedList<StorageEvent<K, V>>();
		var restores = new HashMap<K, V>();
		for (var key : keys ) {
			var oldValue = this.map.get(key);
			var restoredValue = entries.get(key);
			if (oldValue != null) {
				var message = String.format("Cannot restore already presented entries. key: %s, value: %s", key.toString(), oldValue.toString());
				throw new FailedOperationException(message);
			}
			var event = StorageEvent.makeRestoredEntryEvent(this.id, key, restoredValue);
			restores.put(key, restoredValue);
			events.add(event);
		}
		if (0 < restores.size()) {
			this.map.putAll(restores);
		}
		if (0 < events.size()) {
			events.forEach(this.eventDispatcher::accept);
		}
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
