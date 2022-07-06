package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Disposer;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.function.Supplier;

public interface StorageGridTransport {

    static StorageGridTransport create(Observer<byte[]> receiver, Observable<byte[]> sender) {
        var inbound = PublishSubject.<byte[]>create();
        var outbound = PublishSubject.<byte[]>create();
        inbound.subscribe(receiver);
        sender.subscribe(outbound);
        return new StorageGridTransport() {

            @Override
            public Observer<byte[]> getReceiver() {
                return inbound;
            }

            @Override
            public Observable<byte[]> getSender() {
                return outbound;
            }
        };
    }

    Observer<byte[]> getReceiver();
    Observable<byte[]> getSender();

    default void connectTo(StorageGridTransport peer) {
        this.getSender().subscribe(peer.getReceiver());
        peer.getSender().subscribe(this.getReceiver());
    }

    default StorageGridTransport observeOn(Supplier<Scheduler> schedulerSupplier) {
        var sender = PublishSubject.<byte[]>create();
        var receiver = PublishSubject.<byte[]>create();
        var disposer = Disposer.builder().addSubject(sender).addSubject(receiver).build();
        this.getSender().observeOn(schedulerSupplier.get()).subscribe(sender);
        receiver.observeOn(schedulerSupplier.get()).subscribe(this.getReceiver());
        return new StorageGridTransport() {


            @Override
            public Observer<byte[]> getReceiver() {
                return receiver;
            }

            @Override
            public Observable<byte[]> getSender() {
                return sender;
            }
        };
    }
}
