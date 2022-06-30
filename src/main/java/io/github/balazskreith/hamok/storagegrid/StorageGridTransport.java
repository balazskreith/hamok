package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.Disposer;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;

import java.util.List;
import java.util.function.Supplier;

public interface StorageGridTransport extends Disposable {

    static StorageGridTransport create(Observer<List<byte[]>> receiver, Observable<List<byte[]>> sender) {
        var inbound = PublishSubject.<List<byte[]>>create();
        var outbound = PublishSubject.<List<byte[]>>create();
        var disposer = Disposer.builder().addSubject(inbound).addSubject(outbound).build();
        inbound.subscribe(receiver);
        sender.subscribe(outbound);
        return new StorageGridTransport() {
            @Override
            public void dispose() {
                if (!disposer.isDisposed()) {
                    disposer.dispose();
                }
            }

            @Override
            public boolean isDisposed() {
                return disposer.isDisposed();
            }

            @Override
            public Observer<List<byte[]>> getReceiver() {
                return inbound;
            }

            @Override
            public Observable<List<byte[]>> getSender() {
                return outbound;
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
            public void dispose() {
                if (!this.isDisposed()) {
                    this.dispose();
                }
            }

            @Override
            public boolean isDisposed() {
                return this.isDisposed();
            }

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
