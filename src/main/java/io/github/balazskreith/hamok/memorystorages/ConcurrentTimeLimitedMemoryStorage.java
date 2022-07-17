package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.*;
import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.common.UnmodifiableIterator;
import io.github.balazskreith.hamok.rxutils.RxTimeLimitedMap;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ConcurrentTimeLimitedMemoryStorage<K, V> implements Storage<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(ConcurrentTimeLimitedMemoryStorage.class);

	public static<U, R> MemoryStorageBuilder<U, R> builder() {
		return new MemoryStorageBuilder<U, R>()
				.setConcurrency(true);
	}

	private final Disposer disposer;
	private final RxTimeLimitedMap<K, V> map;
	private final RwLock rwLock = new RwLock();
	private final StorageEventDispatcher<K, V> eventDispatcher = new StorageEventDispatcher<>();
	private final String id;

	ConcurrentTimeLimitedMemoryStorage(int expirationTimeInMs) {
		this(UUID.randomUUID().toString(), expirationTimeInMs);
	}

	ConcurrentTimeLimitedMemoryStorage(String id, int expirationTimeInMs) {
		this(id, expirationTimeInMs, Schedulers.computation());
	}

	ConcurrentTimeLimitedMemoryStorage(String id, int expirationTimeInMs, Scheduler scheduler) {
		this.id = id;
		this.map =  new RxTimeLimitedMap<K, V>(expirationTimeInMs);
		this.disposer = Disposer.builder()
				.addDisposable(this.map.expiredEntry()
						.map(entry -> StorageEvent.makeExpiredEntryEvent(this.id, entry.getKey(), entry.getValue()))
						.subscribe(this.eventDispatcher::accept)
				)
				.addDisposable(this.eventDispatcher)
				.build();
		if (scheduler != null) {
			this.disposer.add(scheduler.createWorker().schedulePeriodically(() -> {
				this.rwLock.runInWriteLock(this.map::update);
			}, expirationTimeInMs, expirationTimeInMs, TimeUnit.MILLISECONDS));
		}
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
		this.rwLock.runInWriteLock(() -> {
			var entries = Set.copyOf(this.map.entrySet());
			this.map.clear();
			entries.stream().map(entry -> StorageEvent.makeEvictedEntryEvent(this.id, entry.getKey(), entry.getValue())).forEach(this.eventDispatcher::accept);
		});
	}

	@Override
	public Set<K> keys() {
		return this.rwLock.supplyInReadLock(() -> Set.copyOf(this.map.keySet()));
	}

	public StorageEvents<K, V> events() {
		return this.eventDispatcher;
	}

	@Override
	public boolean isEmpty() {
		return this.rwLock.supplyInReadLock(this.map::isEmpty);
	}

	@Override
	public V get(K key) {
		return this.rwLock.supplyInReadLock(() -> this.map.get(key));
	}

	@Override
	public Map<K, V> getAll(Set<K> keys) {
		return this.rwLock.supplyInReadLock(() -> this.map.getAll(keys));
	}

	@Override
	public V set(K key, V value) {
		return this.rwLock.supplyInWriteLock(() -> {
			var oldValue = this.map.put(key, value);
			StorageEvent<K, V> event;
			if (oldValue == null) {
				event = StorageEvent.makeCreatedEntryEvent(this.id, key, value);
			} else {
				event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, value);
			}
			this.eventDispatcher.accept(event);
			return oldValue;
		});
	}

	@Override
	public Map<K, V> setAll(Map<K, V> map) {
		return this.rwLock.supplyInWriteLock(() -> {
			var keys = map.keySet();
			var result = this.map.getAll(keys);
			this.map.putAll(map);
			for (var key : keys) {
				var oldValue = result.get(key);
				var newValue = map.get(key);
				StorageEvent<K, V> event;
				if (oldValue == null) {
					event = StorageEvent.makeCreatedEntryEvent(this.id, key, newValue);
				} else {
					event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
				}
				this.eventDispatcher.accept(event);
			}
			return Collections.unmodifiableMap(result);
		});
	}

	@Override
	public boolean delete(K key) {
		return this.rwLock.supplyInWriteLock(() -> {
			var oldValue = this.map.remove(key);
			if (oldValue == null) {
				return false;
			}
			var event = StorageEvent.makeDeletedEntryEvent(this.id, key, oldValue);
			this.eventDispatcher.accept(event);
			return true;
		});
	}

	@Override
	public Set<K> deleteAll(Set<K> keys) {
		return this.rwLock.supplyInWriteLock(() -> {
			var result = new HashSet<K>();
			for (var key : keys) {
				var value = this.map.remove(key);
				if (value == null) continue;
				result.add(key);
				var event = StorageEvent.makeDeletedEntryEvent(this.id, key, value);
				this.eventDispatcher.accept(event);
			}
			return Collections.unmodifiableSet(result);
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
		if (this.disposer.isDisposed()) {
			return;
		}
		this.eventDispatcher.accept(StorageEvent.makeClosingStorageEvent(this.id));
		this.disposer.dispose();
		this.rwLock.runInWriteLock(this.map::clear);
	}

	@Override
	public Map<K, V> insertAll(Map<K, V> entries) {
		if (entries == null) return Collections.emptyMap();
		var result = new HashMap<K, V>();
		this.rwLock.runInWriteLock(() -> {
			var it = entries.entrySet().iterator();
			for (; it.hasNext(); ) {
				var entry = it.next();
				var key = entry.getKey();
				var oldValue = this.map.get(key);
				if (oldValue != null) {
					result.put(key, oldValue);
					continue;
				}
				this.map.put(key, entry.getValue());
			}
		});
		return result;
	}
}
