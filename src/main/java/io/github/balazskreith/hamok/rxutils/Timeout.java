package io.github.balazskreith.hamok.rxutils;

interface Timeout {
    static Timeout createVoidTimeout() {
        return new Timeout() {
            @Override
            public long getTimeoutInMs() {
                return 0;
            }

            @Override
            public void start() {

            }

            @Override
            public void stop() {

            }
        };
    }

    long getTimeoutInMs();
    void start();
    void stop();
}
