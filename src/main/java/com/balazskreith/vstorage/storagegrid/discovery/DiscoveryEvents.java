package com.balazskreith.vstorage.storagegrid.discovery;

import com.balazskreith.vstorage.common.Disposer;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;

import java.util.UUID;

public class DiscoveryEvents implements Disposable {
    Subject<UUID> remoteEndpointJoined = PublishSubject.create();
    Subject<UUID> remoteEndpointDetached = PublishSubject.create();
    Subject<Void> localEndpointInactivated = PublishSubject.create();

    private final Disposer disposer;

    DiscoveryEvents() {
        this.disposer = Disposer.builder()
                .addSubject(this.remoteEndpointDetached)
                .addSubject(this.remoteEndpointJoined)
                .build();

    }

    public Observable<UUID> remoteEndpointJoined() {
        return this.remoteEndpointJoined;
    }

    public Observable<UUID> remoteEndpointDetached() {
        return this.remoteEndpointDetached;
    }

    public Observable<Void> localEndpointInactivated() {
        return this.localEndpointInactivated;
    }

    public DiscoveryEvents observeOn(Scheduler scheduler) {
        var result = new DiscoveryEvents();
        this.remoteEndpointJoined.observeOn(scheduler).subscribe(result.remoteEndpointJoined);
        this.remoteEndpointDetached.observeOn(scheduler).subscribe(result.remoteEndpointDetached);
        this.localEndpointInactivated.observeOn(scheduler).subscribe(result.localEndpointInactivated);
        return result;
    }

    @Override
    public void dispose() {
        if (this.disposer.isDisposed()) {
            return;
        }
        this.disposer.dispose();;
    }

    @Override
    public boolean isDisposed() {
        return this.disposer.isDisposed();
    }

}
