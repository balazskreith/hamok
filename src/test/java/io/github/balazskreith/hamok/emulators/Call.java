package io.github.balazskreith.hamok.emulators;

import java.util.UUID;

public record Call(String roomId, UUID callId) {

    public static Call fromString(String str) {
        var a = str.split(",");
        return new Call(
                a[0],
                UUID.fromString(a[1])
        );
    }

    public String toString() {
        return String.format("%s,%s", this.roomId, this.callId);
    }
}
