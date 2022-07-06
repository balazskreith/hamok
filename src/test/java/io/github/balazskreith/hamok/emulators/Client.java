package io.github.balazskreith.hamok.emulators;

import java.util.UUID;

public record Client(UUID clientId) {

    public static Client fromString(String str) {
        var a = str.split(",");
        return new Client(
                UUID.fromString(a[0])
        );
    }

    public String toString() {
        return String.format("%s", this.clientId);
    }
}
