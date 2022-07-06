package io.github.balazskreith.hamok.common;


import java.util.Iterator;
import java.util.Objects;

/**
 * Decorates an iterator such that it cannot be modified.
 *
 * @since Commons Collections 3.0
 * @version $Revision: 646777 $ $Date: 2008-04-10 14:33:15 +0200 (Thu, 10 Apr 2008) $
 *
 * @author Stephen Colebourne
 * @implNote This particular class is copied from Apache commons library
 */
public interface UnmodifiableIterator extends Iterator {

    static UnmodifiableIterator decorate(Iterator iterator) {
        Objects.requireNonNull("Iterator must not be null");
        return new UnmodifiableIterator() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Object next() {
                return iterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("cannot perform remove() operation within Unmodifiable Iterator");
            }
        };
    }

    @Override
    default void remove() {
        throw new UnsupportedOperationException("cannot perform remove() operation within Unmodifiable Iterator");
    }
}