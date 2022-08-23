package io.github.balazskreith.hamok;

/**
 * Indicates an abnormal condition, which renders the Grid or an underlying component incapable of
 * normal workflow without higher level interactions.
 */
public interface HamokError {

    int FAILED_SYNC = 5001;

    static HamokError create(int code, Throwable exception) {
        return new HamokError() {
            @Override
            public int getCode() {
                return code;
            }

            @Override
            public Throwable getException() {
                return exception;
            }
        };
    }


    int getCode();
    Throwable getException();
}
