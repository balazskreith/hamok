package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.*;
import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.common.UnmodifiableIterator;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ConcurrentMemoryStorage<K, V> implements Storage<K, V> {

	public static<U, R> MemoryStorageBuilder<U, R> builder() {
		return new MemoryStorageBuilder<U, R>().setConcurrency(true);
	}

	private final Map<K, V> map = new HashMap<>();
	private final StorageEventDispatcher<K, V> eventDispatcher = new StorageEventDispatcher<>();
	private final RwLock rwLock = new RwLock();
	private final String id;

	ConcurrentMemoryStorage() {
		this(UUID.randomUUID().toString());
	}

	ConcurrentMemoryStorage(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public int size() {
		return this.rwLock.supplyInReadLock(this.map::size);
	}

	@Override
	public void clear() {
		var events = this.rwLock.supplyInReadLock(() -> {
			return this.map.entrySet()
					.stream()
					.map(entry -> StorageEvent.makeEvictedEntryEvent(this.id, entry.getKey(), entry.getValue()))
					.collect(Collectors.toList());
		});
		this.rwLock.runInWriteLock(this.map::clear);
		events.forEach(this.eventDispatcher::accept);
	}

	/**
	 * Returns with a copy of the set of keys the map holds.
	 * @return
	 */
	@Override
	public Set<K> keys() {
		return this.rwLock.supplyInReadLock(() -> Set.copyOf(this.map.keySet()));
	}


	public StorageEvents<K, V> events() {
		return this.eventDispatcher;
	}

	@Override
	public boolean isEmpty() {
		return this.rwLock.supplyInReadLock(() -> this.map.isEmpty());
	}

	@Override
	public V get(K key) {
		return this.rwLock.supplyInReadLock(() -> this.map.get(key));
	}

	@Override
	public Map<K, V> getAll(Set<K> keys) {
		return this.rwLock.supplyInReadLock(() -> {
			var result = new HashMap<K, V>();
			for (var it = keys.iterator(); it.hasNext(); ) {
				var key = it.next();
				var value = this.map.get(key);
				if (value == null) continue;
				result.put(key, value);
			}
			return Collections.unmodifiableMap(result);
		});
	}


	@Override
	public V set(K key, V value) {
		AtomicReference<StorageEvent<K, V>> collectedEvent = new AtomicReference<>(null);
		var result = this.rwLock.supplyInWriteLock(() -> {
			var oldValue = this.map.put(key, value);
			StorageEvent<K, V> event;
			if (oldValue == null) {
				event = StorageEvent.makeCreatedEntryEvent(this.id, key, value);
			} else {
				event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, value);
			}
			collectedEvent.set(event);
			return oldValue;
		});
		try {
			return result;
		} finally {
			var event = collectedEvent.get();
			if (event != null) {
				this.eventDispatcher.accept(event);
			}
		}
	}

	@Override
	public Map<K, V> setAll(Map<K, V> map) {
		var keys = map.keySet();
		var events = new LinkedList<StorageEvent<K, V>>();
		var result = new HashMap<K, V>();
		this.rwLock.runInWriteLock(() -> {
			for (var key : keys ) {
				var oldValue = this.map.get(key);
				var newValue = map.get(key);
				StorageEvent<K, V> event;
				if (oldValue == null) {
					event = StorageEvent.makeCreatedEntryEvent(this.id, key, map.get(key));
				} else {
					event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
					result.put(key, oldValue);
				}
				events.add(event);
			}
			this.map.putAll(map);
		});
		try {
			return Collections.unmodifiableMap(result);
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}
	}

	@Override
	public boolean delete(K key) {
		var oldValue = this.rwLock.supplyInWriteLock(() -> this.map.remove(key));
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
		List<StorageEvent<K, V>> events = new LinkedList<>();
		return this.rwLock.supplyInReadLock(() -> {
			var result = new HashSet<K>();
			for (var it = keys.iterator(); it.hasNext(); ) {
				var key = it.next();
				var value = this.map.remove(key);
				if (value == null) continue;
				var event = StorageEvent.makeDeletedEntryEvent(this.id, key, value);
				events.add(event);
				result.add(key);
			}
			try {
				return Collections.unmodifiableSet(result);
			} finally {
				events.forEach(this.eventDispatcher::accept);
			}

		});
	}


	@Override
	public Iterator<StorageEntry<K, V>> iterator() {
		return this.rwLock.supplyInReadLock(() -> {
			var entryList = this.map.entrySet()
					.stream()
					.map(entry -> StorageEntry.create(entry.getKey(), entry.getValue()))
					.collect(Collectors.toList());
			return UnmodifiableIterator.decorate(entryList.listIterator());
		});
	}

	@Override
	public void close() throws Exception {
		if (!this.eventDispatcher.isDisposed()) {
			this.eventDispatcher.accept(StorageEvent.makeClosingStorageEvent(this.id));
			this.eventDispatcher.dispose();
		}
		this.rwLock.runInWriteLock(this.map::clear);
	}

	@Override
	public Map<K, V> insertAll(Map<K, V> entries) {
		if (entries == null || entries.size() < 1) return Collections.emptyMap();
		var result = new HashMap<K, V>();
		var events = new LinkedList<StorageEvent<K, V>>();
		this.rwLock.runInWriteLock(() -> {
			var it = entries.entrySet().iterator();
			for (; it.hasNext(); ) {
				var entry = it.next();
				var key = entry.getKey();
				var oldValue = this.map.putIfAbsent(key, entry.getValue());
				if (oldValue != null) {
					result.put(key, oldValue);
					continue;
				} else {
					var event = StorageEvent.makeCreatedEntryEvent(this.id, key, entry.getValue());
					events.add(event);
				}
			}
		});
		try {
			return result;
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}
	}
}
