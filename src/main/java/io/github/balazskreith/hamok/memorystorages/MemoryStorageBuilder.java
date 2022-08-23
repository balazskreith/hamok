package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.InvalidConfigurationException;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.BinaryOperator;

/**
 * Represents a builder responsible for building a {@link ConcurrentMemoryStorage}.
 */
public class MemoryStorageBuilder<K, V> {

	private boolean concurrent = false;
	private int expirationTimeInMs = 0;
	private Map<K, V> entries = Collections.emptyMap();
	private Scheduler scheduler;
	private String id = UUID.randomUUID().toString();
	private BinaryOperator<V> mergeOp = (oldValue, newValue) -> newValue;

	public MemoryStorageBuilder<K, V> setEntries(Map<K, V> entries) {
		this.entries = entries;
		return this;
	}

	public MemoryStorageBuilder<K, V> setConcurrency(boolean value) {
		this.concurrent = value;
		return this;
	}

	public MemoryStorageBuilder<K, V> setMergeOp(BinaryOperator<V> mergeOp) {
		this.mergeOp = mergeOp;
		return this;
	}

	public MemoryStorageBuilder<K, V> setId(String value) {
		this.id = value;
		return this;
	}

	public MemoryStorageBuilder<K, V> setExpiration(int timeoutInMs) {
		return this.setExpiration(timeoutInMs, null);
	}

	public MemoryStorageBuilder<K, V> setExpiration(int timeoutInMs, Scheduler scheduler) {
		this.expirationTimeInMs = timeoutInMs;
		this.scheduler = scheduler;
		return this;
	}

	public Storage<K, V> build() {
		Storage<K, V> result;
		if (this.concurrent) {
			if (0 < this.expirationTimeInMs) {
				result = new ConcurrentTimeLimitedMemoryStorage<>(this.id, this.expirationTimeInMs, this.scheduler, this.mergeOp);
			} else {
				result = new ConcurrentMemoryStorage<>(this.id, this.mergeOp);
			}
		} else if (0 < this.expirationTimeInMs) {
			if (this.scheduler != null) {
				throw new InvalidConfigurationException("Scheduler cannot be assigned to a not concurrent time limited map");
			}
			result = new TimeLimitedMemoryStorage<>(this.id, this.expirationTimeInMs, this.mergeOp);
		} else {
			result = new MemoryStorage<>(this.id, this.mergeOp);
		}
		if (0 < this.entries.size()) {
			result.insertAll(this.entries);
		}
		return result;
	}
}
