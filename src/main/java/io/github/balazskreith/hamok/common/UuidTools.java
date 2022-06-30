package io.github.balazskreith.hamok.common;

import java.util.UUID;

public final class UuidTools {

    public static boolean equals(UUID a, UUID b) {
        if (a == null && b == null) return true;
        if (a == null) return false;
        if (b == null) return false;
        if (a.getLeastSignificantBits() != b.getLeastSignificantBits()) return false;
        if (a.getMostSignificantBits() != b.getMostSignificantBits()) return false;
        return true;
    }

    public static boolean notEquals(UUID a, UUID b) {
        return !equals(a, b);
    }
}
