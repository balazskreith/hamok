package io.github.balazskreith.vstorage.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class SetUtils {

    @SafeVarargs
    public static<K> Set<K> addAll(Set<K>... sets) {
        if (sets.length < 1) {
            return Collections.emptySet();
        } else if (sets.length == 1) {
            return sets[0];
        }
        var result = new HashSet<K>();
        Stream.of(sets).forEach(result::addAll);
        return Collections.unmodifiableSet(result);
    }
}
