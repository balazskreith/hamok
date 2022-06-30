package io.github.balazskreith.hamok.mappings;

public interface Encoder<U, R> {

    static<T, U> Encoder<T, U> from(Mapper<T, U> mapper) {
        return mapper::map;
    }

    R encode(U data) throws Throwable;
}