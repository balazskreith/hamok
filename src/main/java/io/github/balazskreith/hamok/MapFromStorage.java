package io.github.balazskreith.hamok;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MapFromStorage<K, V> implements Map<K, V> {

	private final Storage<K, V> storage;
	private final Class<K> keyClass;
	private final Class<V> valueClass;

	public MapFromStorage(Class<K> keyClass, Class<V> valueClass, Storage<K, V> storage) {
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		this.storage = storage;
	}

	@Override
	public int size() {
		return this.storage.size();
	}

	@Override
	public boolean isEmpty() {
		return this.storage.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		Objects.requireNonNull(key, "cannot get null value");
		if (!key.getClass().isAssignableFrom(this.keyClass)) return false;
		return this.storage.get((K)key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		return false;
	}

	@Override
	public V get(Object key) {
		return null;
	}

	@Override
	public V put(K key, V value) {
		return null;
	}

	@Override
	public V remove(Object key) {
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {

	}

	@Override
	public void clear() {

	}

	@Override
	public Set<K> keySet() {
		return null;
	}

	@Override
	public Collection<V> values() {
		return null;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return null;
	}
}
