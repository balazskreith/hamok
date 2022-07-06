package io.github.balazskreith.hamok.common;

public final class Utils {

    public static<T>  T firstNonNull(T... items) {
        if (items == null) return null;
        for (var item : items) {
            if (item == null) continue;
            return item;
        }
        return null;
    }
}
