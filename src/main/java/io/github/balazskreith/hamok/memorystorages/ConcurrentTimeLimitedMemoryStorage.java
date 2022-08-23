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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
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
	private final BinaryOperator<V> mergeOp;

	ConcurrentTimeLimitedMemoryStorage(int expirationTimeInMs) {
		this(UUID.randomUUID().toString(), expirationTimeInMs);
	}

	ConcurrentTimeLimitedMemoryStorage(String id, int expirationTimeInMs) {
		this(id, expirationTimeInMs, Schedulers.computation());
	}

	ConcurrentTimeLimitedMemoryStorage(String id, int expirationTimeInMs, Scheduler scheduler) {
		this(id, expirationTimeInMs, scheduler, (oldValue, newValue) -> newValue);
	}

	ConcurrentTimeLimitedMemoryStorage(String id, int expirationTimeInMs, Scheduler scheduler, BinaryOperator<V> mergeOp) {
		this.id = id;
		this.map =  new RxTimeLimitedMap<K, V>(expirationTimeInMs);
		this.mergeOp = mergeOp;
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
		var events = this.rwLock.supplyInWriteLock(() -> {
			var result = this.map.entrySet()
					.stream()
					.map(entry -> StorageEvent.makeEvictedEntryEvent(this.id, entry.getKey(), entry.getValue()))
					.collect(Collectors.toList());
			this.map.clear();
			return result;
		});
		events.forEach(this.eventDispatcher::accept);
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
		var event = new AtomicReference<StorageEvent<K, V>>();
		var oldValue = this.rwLock.supplyInWriteLock(() -> {
			var result = this.map.get(key);
			if (result == null) {
				event.set(StorageEvent.makeCreatedEntryEvent(this.id, key, value));
				return this.map.put(key, value);
			}
			var newValue = this.mergeOp.apply(result, value);
			event.set(StorageEvent.makeUpdatedEntryEvent(this.id, key, result, newValue));
			return this.map.put(key, newValue);
		});
		try {
			return oldValue;
		} finally {
			this.eventDispatcher.accept(event.get());
		}
	}

	@Override
	public Map<K, V> setAll(Map<K, V> map) {
		var events = new LinkedList<StorageEvent<K, V>>();
		Map<K, V> result = this.rwLock.supplyInWriteLock(() -> {
			var inserts = new HashMap<K, V>();
			var updates = new HashMap<K, V>();
			var keys = map.keySet();
			var entries = this.map.getAll(keys);
			for (var key : keys) {
				var oldValue = entries.get(key);
				var value = map.get(key);
				StorageEvent<K, V> event;
				if (oldValue == null) {
					inserts.put(key, value);
					event = StorageEvent.makeCreatedEntryEvent(this.id, key, value);
				} else {
					var newValue = this.mergeOp.apply(oldValue, value);
					updates.put(key, newValue);
					event = StorageEvent.makeUpdatedEntryEvent(this.id, key, oldValue, newValue);
				}
				events.add(event);
			}
			if (0 < inserts.size()) {
				this.map.putAll(inserts);
			}
			if (0 < updates.size()) {
				this.map.putAll(updates);
			}
			return Collections.<K, V>unmodifiableMap(entries);
		});
		try {
			return result;
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}
	}


	@Override
	public void restore(K key, V value) {
		var event = this.rwLock.supplyInWriteLock(() -> {
			if (this.map.containsKey(key)) {
				var message = String.format("Cannot restore already presented entries. key: %s, value: %s", key.toString(), value.toString());
				throw new FailedOperationException(message);
			}
			this.map.put(key, value);
			return StorageEvent.makeRestoredEntryEvent(this.id, key, value);
		});
		this.eventDispatcher.accept(event);
	}

	@Override
	public void restoreAll(Map<K, V> entries) {
		var keys = entries.keySet();
		var events = this.rwLock.supplyInWriteLock(() -> {
			var result = new LinkedList<StorageEvent<K, V>>();
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
				result.add(event);
			}
			if (0 < restores.size()) {
				this.map.putAll(restores);
			}
			return result;
		});
		if (0 < events.size()) {
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
		var events = new LinkedList<StorageEvent<K, V>>();
		var result = this.rwLock.supplyInWriteLock(() -> {
			var entries = new HashSet<K>();
			for (var key : keys) {
				var value = this.map.remove(key);
				if (value == null) continue;
				entries.add(key);
				var event = StorageEvent.makeDeletedEntryEvent(this.id, key, value);
				events.add(event);
			}
			return Collections.unmodifiableSet(entries);
		});
		try {
			return result;
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}
	}

	@Override
	public void evictAll(Set<K> keys) {
		List<StorageEvent<K, V>> events = new LinkedList<>();
		this.rwLock.runInWriteLock(() -> {
			for (var it = keys.iterator(); it.hasNext(); ) {
				var key = it.next();
				var value = this.map.remove(key);
				if (value == null) continue;
				var event = StorageEvent.makeEvictedEntryEvent(this.id, key, value);
				events.add(event);
			}
		});
		events.forEach(this.eventDispatcher::accept);
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
		if (entries == null || entries.size() < 1) return Collections.emptyMap();
		var events = new LinkedList<StorageEvent<K, V>>();
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
				} else {
					var event = StorageEvent.makeCreatedEntryEvent(this.id, key, entry.getValue());
					events.add(event);
				}
				this.map.put(key, entry.getValue());
			}
		});
		try {
			return result;
		} finally {
			events.forEach(this.eventDispatcher::accept);
		}

	}
}
