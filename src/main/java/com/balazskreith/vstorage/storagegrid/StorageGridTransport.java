package com.balazskreith.vstorage.storagegrid;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.List;
import java.util.function.Supplier;

public interface StorageGridTransport {

    static StorageGridTransport create(Observer<List<byte[]>> receiver, Observable<List<byte[]>> sender) {
        return new StorageGridTransport() {
            @Override
            public Observer<List<byte[]>> getReceiver() {
                return receiver;
            }

            @Override
            public Observable<List<byte[]>> getSender() {
                return sender;
            }
        };
    }

    Observer<List<byte[]>> getReceiver();
    Observable<List<byte[]>> getSender();

    default void connectTo(StorageGridTransport peer) {
        this.getSender().subscribe(peer.getReceiver());
        peer.getSender().subscribe(this.getReceiver());
    }

    default StorageGridTransport observeOn(Supplier<Scheduler> schedulerSupplier) {
        var sender = PublishSubject.<List<byte[]>>create();
        var receiver = PublishSubject.<List<byte[]>>create();
        this.getSender().observeOn(schedulerSupplier.get()).subscribe(sender);
        receiver.observeOn(schedulerSupplier.get()).subscribe(this.getReceiver());
        return new StorageGridTransport() {
            @Override
            public Observer<List<byte[]>> getReceiver() {
                return receiver;
            }

            @Override
            public Observable<List<byte[]>> getSender() {
                return sender;
            }
        };
    }
}
