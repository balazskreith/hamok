package io.github.balazskreith.hamok.rxutils;


import io.github.balazskreith.hamok.common.InvalidConfigurationException;
import io.github.balazskreith.hamok.common.KeyValuePair;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A special LinkedHasMap created for time limited storage of items
 * @param <K>
 * @param <V>
 */
public class RxTimeLimitedMap<K, V> extends HashMap<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(RxTimeLimitedMap.class);

    private final Map<K, Long> accessedKeys;
    private final long maxTimeInMs;
    private final Subject<KeyValuePair<K, V>> expiredEntry;

    public RxTimeLimitedMap(int maxTimeInMs) {
        if (maxTimeInMs < 1) {
            throw new InvalidConfigurationException("Cannot create a time limited map without a time limitation. given maxTimeInMs: " + maxTimeInMs);
        }
        this.accessedKeys = new LinkedHashMap<>(16, .75f, true);
        this.maxTimeInMs = maxTimeInMs;
        this.expiredEntry = PublishSubject.create();
    }

    public Observable<KeyValuePair<K, V>> expiredEntry() {
        return this.expiredEntry;
    }

    @Override
    public void clear() {
        this.accessedKeys.clear();
        super.clear();
    }

    @Override
    public V put(K key, V value) {
        V result = super.put(key, value);
        var now = Instant.now().toEpochMilli();
        this.accessedKeys.put(key, now);
        this.update(now);
        return result;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        super.putAll(m);
        var now = Instant.now().toEpochMilli();
        var accesses = m.keySet().stream().collect(Collectors.toMap(
                Function.identity(),
                e -> now
        ));
        this.accessedKeys.putAll(accesses);
        this.update(now);
    }

    @Override
    public V remove(Object key) {
        V removed = super.remove(key);
        if (Objects.isNull(removed)) {
            return null;
        }
        return removed;
    }

    @Override
    public V get(Object key) {
        V value = super.get(key);
        if (Objects.isNull(value)) {
            return null;
        }
        var now = Instant.now().toEpochMilli();

        this.accessedKeys.put((K) key, now);
        this.update(now);
        return value;
    }

    public Map<K, V> getAll(Set<K> keys) {
        var result = new HashMap<K, V>();
        var now = Instant.now().toEpochMilli();
        for (var it = keys.iterator(); it.hasNext(); ) {
            var key = it.next();
            var value = super.get(key);
            if (value == null) continue;
            result.put(key, value);
            this.accessedKeys.put(key, now);
        }
        this.update(now);
        return Collections.unmodifiableMap(result);
    }


    public void update() {
        this.update(Instant.now().toEpochMilli());
    }

    private void update(long now) {
        Iterator<Entry<K, Long>> it = this.accessedKeys.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<K, Long> entry = it.next();
            var elapsedTimeInMs = now - entry.getValue();

            if (elapsedTimeInMs < this.maxTimeInMs) {
                // the first item, accessed less than threshold, then we stop the check because
                // we know all consecutive items are accessed less than this.
                break;
            }
            // no hard feelings
            var key = entry.getKey();
            var removedValue = this.remove(key);

            // here we remove it from the accessedKeys
            it.remove();
            var removedKeyValuePair = KeyValuePair.of(key, removedValue);
            this.expiredEntry.onNext(removedKeyValuePair);
        }
    }
}
