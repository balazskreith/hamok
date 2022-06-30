package io.github.balazskreith.hamok.storagegrid.messages;

import java.util.UUID;

public record SubmitResponse(
        UUID requestId,
        UUID destinationId,
        boolean success,
        UUID leaderId
        )
{

}
