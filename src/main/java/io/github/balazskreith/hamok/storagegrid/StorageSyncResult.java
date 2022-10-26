package io.github.balazskreith.hamok.storagegrid;

import java.util.List;

public record StorageSyncResult(
        boolean success,
        List<String> errors
) {
}
