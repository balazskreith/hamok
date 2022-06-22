package com.balazskreith.vstorage.storagegrid;

import java.util.UUID;

public record StorageGridConfig(
        UUID localEndpointId,
        int requestTimeoutInMs
        )
{

}
