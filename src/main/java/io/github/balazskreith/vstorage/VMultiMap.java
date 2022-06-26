package io.github.balazskreith.vstorage;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class VMultiMap<K, V> {

	private final Storage<K, Set<V>> storage;

	public VMultiMap(Storage<K, Set<V>> storage) {
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

	public boolean containsValue(K key, V value) {
		var set = this.storage.get(key);
		if (set == null) {
			return false;
		}
		return set.contains(value);
	}

	public Set<V> get(Object key) {
		return this.storage.get((K) key).stream().collect(Collectors.toSet());
	}

	public boolean put(K key, V value) {
		var set = this.storage.get(key);
		if (set == null) {
			set = new HashSet<>();
		}
		int sizeBefore = set.size();
		set.add(value);
		int sizeAfter = set.size();
		this.storage.put(key, set);
		return sizeBefore != sizeAfter;
	}

	public boolean remove(K key, V value) {
		var set = this.storage.get(key);
		if (set == null) {
			return false;
		}
		int sizeBefore = set.size();
		set.remove(value);
		int sizeAfter = set.size();
		this.storage.put(key, set);
		return sizeBefore != sizeAfter;
	}

	public void clear() {
		this.storage.clear();
	}

	public Set<K> keySet() {
		Set<K> result = new HashSet<>();
		for (var it = this.storage.iterator(); it.hasNext(); ) {
			var entry = it.next();
			var key = entry.getKey();
			result.add(key);
		}
		return result;
	}

}
