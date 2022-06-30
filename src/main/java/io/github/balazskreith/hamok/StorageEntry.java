package io.github.balazskreith.hamok;

import java.util.Map;
import java.util.Objects;

public interface StorageEntry<K, V> {
    static<U, R> StorageEntry<U, R> createFromMapEntry(Map.Entry<U, R> entry) {
        return StorageEntry.<U, R>create(entry.getKey(), entry.getValue());
    }
    static<U, R> StorageEntry<U, R> create(U key, R value) {
        return new StorageEntry<>() {
            @Override
            public U getKey() {
                return key;
            }

            @Override
            public R getValue() {
                return value;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(key) ^ Objects.hashCode(value);
            }

            @Override
            public boolean equals(Object o) {
                if (o == this)
                    return true;
                if (o instanceof StorageEntry<?,?> == false) {
                    return false;
                }
                return Objects.equals(key, this.getKey()) && Objects.equals(value, this.getValue());
            }
        };
    }
    /**
     * Returns the key corresponding to this entry.
     *
     */
    K getKey();

    /**
     * Returns the value corresponding to this entry.  If the mapping
     */
    V getValue();

    /**
     * Compares the specified object with this entry for equality.
     * Returns {@code true} if the given object is also a map entry and
     * the two entries represent the same mapping.  More formally, two
     * entries {@code e1} and {@code e2} represent the same mapping
     * if<pre>
     *     (e1.getKey()==null ?
     *      e2.getKey()==null : e1.getKey().equals(e2.getKey()))  &amp;&amp;
     *     (e1.getValue()==null ?
     *      e2.getValue()==null : e1.getValue().equals(e2.getValue()))
     * </pre>
     * This ensures that the {@code equals} method works properly across
     * different implementations of the {@code Map.Entry} interface.
     *
     * @param o object to be compared for equality with this map entry
     * @return {@code true} if the specified object is equal to this map
     *         entry
     */
    boolean equals(Object o);


    int hashCode();

}
