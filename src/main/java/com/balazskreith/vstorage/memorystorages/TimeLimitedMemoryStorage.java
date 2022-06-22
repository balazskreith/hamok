package com.balazskreith.vstorage.memorystorages;

import com.balazskreith.vstorage.*;
import com.balazskreith.vstorage.common.Disposer;
import com.balazskreith.vstorage.rxutils.RxTimeLimitedMap;

import java.util.*;
import java.util.stream.Collectors;

public class TimeLimitedMemoryStorage<K, V> implements Storage<K, V> {

	public static<U, R> MemoryStorageBuilder<U, R> builder() {
		return new MemoryStorageBuilder<U, R>();
	}

	private final Disposer disposer;
	private final RxTimeLimitedMap<K, V> map;
	private final StorageEventDispatcher<K, V> eventDispatcher = new StorageEventDispatcher<>();
	private final String id;

	TimeLimitedMemoryStorage(int expirationTimeInMs) {
		this(UUID.randomUUID().toString(), expirationTimeInMs);
	}

	TimeLimitedMemoryStorage(String id, int expirationTimeInMs) {
		this.id = id;
		this.map = new RxTimeLimitedMap<K, V>(expirationTimeInMs);
		this.disposer = Disposer.builder()
				.addDisposable(this.map.expiredEntry()
						.map(entry -> StorageEvent.makeExpiredEntryEvent(this.id, entry.getKey(), entry.getValue()))
						.subscribe(this.eventDispatcher::accept)
				)
				.addDisposable(this.eventDispatcher)
				.build();
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
		var entries = Set.copyOf(this.map.entrySet());
		this.map.clear();
		entries.stream().map(entry -> StorageEvent.makeDeletedEntryEvent(this.id, entry.getKey(), entry.getValue())).forEach(this.eventDispatcher::accept);
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
		for (var it = keys.iterator(); it.hasNext(); ) {
			var key = it.next();
			var value = this.map.get(key);
			if (value == null) continue;
			result.put(key, value);
		}
		return result;
	}


	@Override
	public V put(K key, V value) {
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
	public Map<K, V> putAll(Map<K, V> map) {
		var keys = map.keySet();
		var result = this.getAll(keys);
		this.map.putAll(map);
		for (var key : keys) {
			var oldValue = result.get(key);
			var newValue = this.map.get(key);
			StorageEvent<K, V> event;
			if (oldValue == null) {
				event = StorageEvent.makeCreatedEntryEvent(this.id, key, newValue);
			} else {
				event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
			}
			this.eventDispatcher.accept(event);
		}
		return result;
	}

	@Override
	public boolean delete(K key) {
		var oldValue = this.map.remove(key);
		if (oldValue == null) {
			return false;
		}
		var event = StorageEvent.makeDeletedEntryEvent(this.id, key, oldValue);
		this.eventDispatcher.accept(event);
		return true;
	}

	@Override
	public Set<K> deleteAll(Set<K> keys) {
		return keys.stream().filter(this::delete).collect(Collectors.toSet());
	}

	@Override
	public void evict(K key) {
		var oldValue =  this.map.remove(key);
		if (oldValue == null) {
			return;
		}
		var event = StorageEvent.makeEvictedEntryEvent(this.id, key, oldValue);
		this.eventDispatcher.accept(event);
	}

	@Override
	public void evictAll(Set<K> keys) {
		for (var it = keys.iterator(); it.hasNext(); ) {
			var key = it.next();
			var oldValue =  this.map.remove(key);
			if (oldValue == null) {
				return;
			}
			var event = StorageEvent.makeEvictedEntryEvent(this.id, key, oldValue);
			this.eventDispatcher.accept(event);
		}
	}

	@Override
	public Iterator<StorageEntry<K, V>> iterator() {
		var entryList = this.map.entrySet()
				.stream()
				.map(entry -> StorageEntry.create(entry.getKey(), entry.getValue()))
				.collect(Collectors.toList());
		return entryList.iterator();
	}

	@Override
	public void close() throws Exception {
		if (!this.eventDispatcher.isDisposed()) {
			this.eventDispatcher.accept(StorageEvent.makeClosingStorageEvent(this.id));
			this.eventDispatcher.dispose();
		}
		this.map.clear();
	}
}
