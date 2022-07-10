package io.github.balazskreith.hamok.storagegrid;

public class FailedOperationException extends RuntimeException {
    FailedOperationException(String message) {
        super(message);
    }
}
