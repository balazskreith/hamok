package com.balazskreith.vstorage.rxutils;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.subjects.Subject;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class RxAtomicReference<T> extends Observable<Optional<T>> {

    private final Subject<Optional<T>> subject = PublishSubject.create();
    private final AtomicReference<T> reference = new AtomicReference<>(null);

    public RxAtomicReference(T initialValue) {
        this.reference.set(initialValue);
    }

    public void set(T newValue) {
        var oldValue = reference.getAndSet(newValue);
        if (oldValue == newValue) {
            return;
        }
        if (newValue == null) {
            this.subject.onNext(Optional.empty());
        } else {
            this.subject.onNext(Optional.of(newValue));
        }
    }

    public T get() {
        return this.reference.get();
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super Optional<T>> observer) {
        this.subject.subscribe(observer);
    }
}
