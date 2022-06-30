package io.github.balazskreith.hamok.storagegrid;

import java.util.UUID;

public record StorageGridConfig(
        UUID localEndpointId,
        int requestTimeoutInMs
        )
{

}
