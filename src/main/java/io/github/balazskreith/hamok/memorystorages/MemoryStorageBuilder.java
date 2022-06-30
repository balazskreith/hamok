package io.github.balazskreith.hamok.memorystorages;

import io.github.balazskreith.hamok.Storage;
import io.github.balazskreith.hamok.common.InvalidConfigurationException;
import io.reactivex.rxjava3.core.Scheduler;

import java.util.UUID;

/**
 * Represents a builder responsible for building a {@link ConcurrentMemoryStorage}.
 */
public class MemoryStorageBuilder<K, V> {

	private boolean concurrent = false;
	private int expirationTimeInMs = 0;
	private Scheduler scheduler;
	private String id = UUID.randomUUID().toString();

	public MemoryStorageBuilder<K, V> setConcurrency(boolean value) {
		this.concurrent = value;
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
				result = new ConcurrentTimeLimitedMemoryStorage<>(this.id, this.expirationTimeInMs, this.scheduler);
			} else {
				result = new ConcurrentMemoryStorage<>(this.id);
			}
		} else if (0 < this.expirationTimeInMs) {
			if (this.scheduler != null) {
				throw new InvalidConfigurationException("Scheduler cannot be assigned to a not concurrent time limited map");
			}
			result = new TimeLimitedMemoryStorage<>(this.id, this.expirationTimeInMs);
		} else {
			result = new MemoryStorage<>(this.id);
		}
		return result;
	}
}
