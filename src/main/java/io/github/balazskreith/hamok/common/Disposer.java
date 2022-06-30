package io.github.balazskreith.hamok.common;

import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Helper class to dispose observables, observers, and other stuffs only once!
 */
public class Disposer implements Disposable {

    private static final Logger logger = LoggerFactory.getLogger(Disposer.class);

    public static Builder builder() {
        return new Builder();
    }

    private volatile boolean disposed = false;
    private final CompositeDisposable disposables = new CompositeDisposable();

//    private List<Disposable> disposables = new LinkedList<>();
    private Runnable onCompleted;

    private Disposer() {

    }

    public void add(Disposable disposable) {
        synchronized (this) {
            this.disposables.add(disposable);
        }
    }

    public boolean remove(Disposable disposable) {
        return this.disposables.delete(disposable);
    }

    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }
        this.disposed = true;
        this.disposables.dispose();
    }

    @Override
    public boolean isDisposed() {
        return this.disposed;
    }

    public static class Builder {
        private Disposer result = new Disposer();
        private Builder() {

        }

        public Builder addSubject(Subject subject) {
            var disposable = makeDisposable(subject::onComplete, () -> subject.hasComplete() || subject.hasThrowable());
            this.result.disposables.add(disposable);
            return this;
        }

        public Builder addDisposable(Disposable disposable) {
            this.result.disposables.add(disposable);
            return this;
        }

        public Builder addDisposable(Supplier<Boolean> isDisposedSupplier, Runnable disposeAction) {
            var disposable = makeDisposable(disposeAction, isDisposedSupplier);
            this.result.disposables.add(disposable);
            return this;
        }

        public Builder onCompleted(Runnable onCompleted) {
            this.result.onCompleted = onCompleted;
            return this;
        }

        public Disposer build() {
            return this.result;
        }
    }

    private static Disposable makeDisposable(Runnable disposeAction, Supplier<Boolean> isDisposedSupplier) {
        return new Disposable() {
            @Override
            public void dispose() {
                disposeAction.run();
            }

            @Override
            public boolean isDisposed() {
                return isDisposedSupplier.get();
            }
        };
    }
}
