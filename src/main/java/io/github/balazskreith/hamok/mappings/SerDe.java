package io.github.balazskreith.hamok.mappings;

public interface SerDe<T>  {
    byte[] serialize(T object);
    T deserialize(byte[] data);
}
