package io.github.balazskreith.hamok.transports;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;

public interface Endpoint {
    Observable<Message> inboundChannel();
    Observer<Message> outboundChannel();

    void start();
    boolean isRunning();
    void stop();
}
