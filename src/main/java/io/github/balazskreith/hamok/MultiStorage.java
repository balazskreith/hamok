package io.github.balazskreith.hamok;

import java.util.Collection;
import java.util.Map;

// scope is very important!
// delete is atomic locally, blablabla...

/**
 * General interface to represent a storage.
 *
 *
 * @param <K>
 * @param <V>
 */
public interface MultiStorage<K, V, U extends Collection<V>> extends Storage<K, U> {

    U add(K key, V... values);

    Map<K, U> addAll(Map<K, U> entries);

    boolean contains(K key, V... values);

    boolean containsAll(Map<K, U> entries);

    boolean containsAny(Map<K, U> entries);

    U remove(K key, V... values);

    Map<K, U> removeAll(Map<K, U> entries);


}
