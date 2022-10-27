package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.Models;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;

public interface Endpoint {
    Observable<Models.Message> inboundChannel();
    Observer<Models.Message> outboundChannel();

    void start();
    boolean isRunning();
    void stop();
}
