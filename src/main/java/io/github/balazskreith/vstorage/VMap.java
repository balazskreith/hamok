package io.github.balazskreith.vstorage;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class VMap<K, V> implements Map<K, V> {

	private final Storage<K, V> storage;

	public VMap(Storage<K, V> storage) {
		this.storage = storage;
	}

	public int size() {
		return this.storage.size();
	}

	public boolean isEmpty() {
		return this.storage.isEmpty();
	}

	public boolean containsKey(Object key) {
		return this.storage.get((K) key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		return false;
	}

	public V get(Object key) {
		return this.storage.get((K) key);
	}

	public Map<K, V> getAll(Set<K> keys) {
		return this.storage.getAll(keys);
	}

	public V put(K key, V value) {
		var oldValue = this.storage.put(key, value);
		return oldValue;
	}

	@Override
	public V remove(Object key) {
		K keyObj = (K) key;
		V result = this.storage.get(keyObj);
		this.storage.delete(keyObj);
		return result;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		Map<K, V> entries = m.entrySet().stream().collect(Collectors.toMap(
				Map.Entry::getKey,
				Map.Entry::getValue
		));
		this.storage.putAll(entries);
	}


	public V putIfAbsent(K key, V value) {
		var actualValue = this.storage.get(key);
		if (actualValue != null) {
			return actualValue;
		}
		this.put(key, value);
		return null;
	}


	public void clear() {
		this.storage.clear();
	}

	public V getOrDefault(Object key, V defaultValue) {
		if (this.storage.get((K) key) == null) {
			return defaultValue;
		}
		return this.storage.get((K) key);
	}

	public Stream<K> streamKeys() {
		return StreamSupport.stream(
				Spliterators.spliterator(this.storage.iterator(), this.storage.size(), Spliterator.ORDERED),
				false
		).map(StorageEntry::getKey);
	}

	public Set<K> keySet() {
		var result = new HashSet<K>();
		for (var it = this.storage.iterator(); it.hasNext(); ) {
			var entry = it.next();
			var key = entry.getKey();
			result.add(key);
		}
		return result;
	}

	public List<V> values() {
		var result = new LinkedList<V>();
		for (var it = this.storage.iterator(); it.hasNext(); ) {
			var entry = it.next();
			var value = entry.getValue();
			result.add(value);
		}
		return result;
	}

	public Set<Map.Entry<K, V>> entrySet() {
		var result = new HashMap<K, V>();
		for (var it = this.storage.iterator(); it.hasNext(); ) {
			var entry = it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result.entrySet();
	}

	public void forEach(BiConsumer<? super K, ? super V> action) {
		for (Iterator<StorageEntry<K, V>> it = this.storage.iterator(); it.hasNext(); ) {
			var entry = it.next();
			K key = entry.getKey();
			V value = entry.getValue();
			action.accept(key, value);
		}
	}
}
