package io.github.balazskreith.hamok.common;

import java.util.Iterator;

public final class Utils {
    private static Iterator EMPTY_ITERATOR = new Iterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object next() {
            return null;
        }
    };

    public static<T> Iterator<T> emptyIterator() {
        return EMPTY_ITERATOR;
    }

    public static<T>  T firstNonNull(T... items) {
        if (items == null) return null;
        for (var item : items) {
            if (item == null) continue;
            return item;
        }
        return null;
    }
}
