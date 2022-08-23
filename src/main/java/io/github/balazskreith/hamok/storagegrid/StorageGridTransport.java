package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.Disposer;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.function.Supplier;

public interface StorageGridTransport {

    static StorageGridTransport create(Observer<Models.Message> receiver, Observable<Models.Message> sender) {
        var inbound = PublishSubject.<Models.Message>create();
        var outbound = PublishSubject.<Models.Message>create();
        inbound.subscribe(receiver);
        sender.subscribe(outbound);
        return new StorageGridTransport() {

            @Override
            public Observer<Models.Message> getReceiver() {
                return inbound;
            }

            @Override
            public Observable<Models.Message> getSender() {
                return outbound;
            }
        };
    }

    Observer<Models.Message> getReceiver();
    Observable<Models.Message> getSender();

    default void connectTo(StorageGridTransport peer) {
        this.getSender().subscribe(peer.getReceiver());
        peer.getSender().subscribe(this.getReceiver());
    }

    default StorageGridTransport observeOn(Supplier<Scheduler> schedulerSupplier) {
        var sender = PublishSubject.<Models.Message>create();
        var receiver = PublishSubject.<Models.Message>create();
        var disposer = Disposer.builder().addSubject(sender).addSubject(receiver).build();
        this.getSender().observeOn(schedulerSupplier.get()).subscribe(sender);
        receiver.observeOn(schedulerSupplier.get()).subscribe(this.getReceiver());
        return new StorageGridTransport() {


            @Override
            public Observer<Models.Message> getReceiver() {
                return receiver;
            }

            @Override
            public Observable<Models.Message> getSender() {
                return sender;
            }
        };
    }
}
