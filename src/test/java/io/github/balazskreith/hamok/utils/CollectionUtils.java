package io.github.balazskreith.hamok.utils;

import java.util.Collection;
import java.util.Objects;

public class CollectionUtils {

    public static<T> boolean equalCollections(Collection<T> a, Collection<T> b) {
        if (a == null && b == null) return true;
        if (a == null) return false;
        if (b == null) return false;
        if (a.size() != b.size()) return false;
        for (var it = a.iterator(); it.hasNext(); ) {
            var aItem = it.next();
            var found = b.stream().filter(bItem -> Objects.equals(aItem, bItem)).findFirst();
            if (found.isEmpty()) {
                return false;
            }
        }
        for (var it = b.iterator(); it.hasNext(); ) {
            var bItem = it.next();
            var found = a.stream().filter(aItem -> Objects.equals(aItem, bItem)).findFirst();
            if (found.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
