package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Disposer;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.function.Supplier;

public interface StorageGridTransport {

    static StorageGridTransport create(Observer<Message> receiver, Observable<Message> sender) {
        var inbound = PublishSubject.<Message>create();
        var outbound = PublishSubject.<Message>create();
        inbound.subscribe(receiver);
        sender.subscribe(outbound);
        return new StorageGridTransport() {

            @Override
            public Observer<Message> getReceiver() {
                return inbound;
            }

            @Override
            public Observable<Message> getSender() {
                return outbound;
            }
        };
    }

    Observer<Message> getReceiver();
    Observable<Message> getSender();

    default void connectTo(StorageGridTransport peer) {
        this.getSender().subscribe(peer.getReceiver());
        peer.getSender().subscribe(this.getReceiver());
    }

    default StorageGridTransport observeOn(Supplier<Scheduler> schedulerSupplier) {
        var sender = PublishSubject.<Message>create();
        var receiver = PublishSubject.<Message>create();
        var disposer = Disposer.builder().addSubject(sender).addSubject(receiver).build();
        this.getSender().observeOn(schedulerSupplier.get()).subscribe(sender);
        receiver.observeOn(schedulerSupplier.get()).subscribe(this.getReceiver());
        return new StorageGridTransport() {


            @Override
            public Observer<Message> getReceiver() {
                return receiver;
            }

            @Override
            public Observable<Message> getSender() {
                return sender;
            }
        };
    }
}
