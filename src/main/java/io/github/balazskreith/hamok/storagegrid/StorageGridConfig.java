package io.github.balazskreith.hamok.storagegrid;

public record StorageGridConfig(
        int requestTimeoutInMs
        )
{
        public static StorageGridConfig create() {
                return new StorageGridConfig(
                        3000
                );
        }

        public StorageGridConfig copyAndSet(int requestTimeoutInMs) {
                return new StorageGridConfig(
                        requestTimeoutInMs
                );
        }
}
