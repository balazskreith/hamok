package io.github.balazskreith.vstorage.mappings;

public interface SerDe<T>  {
    byte[] serialize(T object);
    T deserialize(byte[] data);
}
