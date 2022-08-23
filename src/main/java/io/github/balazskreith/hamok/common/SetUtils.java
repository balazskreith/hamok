package io.github.balazskreith.hamok.common;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class SetUtils {

    @SafeVarargs
    public static<K> Set<K> combineAll(Set<K>... sets) {
        if (sets.length < 1) {
            return Collections.emptySet();
        } else if (sets.length == 1) {
            return Collections.unmodifiableSet(sets[0]);
        }
        var result = new HashSet<K>();
        Stream.of(sets).forEach(result::addAll);
        return Collections.unmodifiableSet(result);
    }

    public static<T> boolean isContentsEqual(Set<T> a, Set<T> b) {
        if (a == null && b == null) return true;
        if (a == null) return false;
        if (b == null) return false;
        if (a.size() != b.size()) return false;
        for (var it = a.iterator(); it.hasNext(); ) {
            var item = it.next();
            if (!b.contains(item)) return false;
        }

        for (var it = b.iterator(); it.hasNext(); ) {
            var item = it.next();
            if (!a.contains(item)) return false;
        }

        return true;
    }
}
