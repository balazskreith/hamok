package io.github.balazskreith.vstorage.raft;

import io.github.balazskreith.vstorage.raft.events.RaftAppendEntriesRequest;
import io.github.balazskreith.vstorage.raft.events.RaftAppendEntriesResponse;
import io.github.balazskreith.vstorage.raft.events.RaftVoteRequest;
import io.github.balazskreith.vstorage.raft.events.RaftVoteResponse;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.function.Consumer;

abstract class AbstractActor implements Actor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractActor.class);

    private final Object oneMessage = new Object();
    private final CompositeDisposable disposer = new CompositeDisposable();
    protected RxRaft raft;
    private final UUID id;
    private volatile boolean disposed = false;

    protected AbstractActor(RxRaft raft) {
        this.raft = raft;
        this.id = this.raft.getId();
    }

    protected AbstractActor addDisposable(Disposable disposable) {
        this.disposer.add(disposable);
        return this;
    }
    protected String getId() {
        return this.id.toString().substring(0, 8);
    }

    @Override
    public void stop() {
        if (!this.isDisposed()) {
            logger.info("{} state {} is being stopped",  this.raft.config().id(), this.getState().name());
            this.dispose();
        }
    }

    protected AbstractActor onVoteRequested(Consumer<RaftVoteRequest> listener) {
        this.addDisposable(this.raft.inboundEvents().voteRequests().subscribe(request -> {
            if (this.disposed) {
                logger.warn("{} received {} after it is stopped", this.id, request);
                return;
            }
            synchronized (this.oneMessage) {
                listener.accept(request);
            }
        }));
        return this;
    }

    protected void sendVoteRequest(RaftVoteRequest request) {
        if (this.disposed) {
            logger.warn("{} tried to send {} after it is stopped", this.id, request);
            return;
        }
        this.raft.outboundEvents().voteRequests().onNext(request);
    }

    protected AbstractActor onVoteResponse(Consumer<RaftVoteResponse> listener) {
        this.addDisposable(this.raft.inboundEvents().voteResponse().subscribe(response -> {
            if (this.disposed) {
                logger.warn("{} received {} after it is stopped", this.id, response);
                return;
            }
            synchronized (this.oneMessage) {
                listener.accept(response);
            }
        }));
        return this;
    }

    protected void sendVoteResponse(RaftVoteResponse response) {
        if (this.disposed) {
            logger.warn("{} tried to send {} after it is stopped", this.id, response);
            return;
        }
        this.raft.outboundEvents().voteResponse().onNext(response);
    }

    protected AbstractActor onAppendEntriesRequest(Consumer<RaftAppendEntriesRequest> listener) {
        this.addDisposable(this.raft.inboundEvents().appendEntriesRequest().subscribe(request -> {
            if (this.disposed) {
                logger.warn("{} received {} after it is stopped", this.id, request);
                return;
            }
            // make sure execution is sequential
            synchronized (this.oneMessage) {
                listener.accept(request);
            }
        }));
        return this;
    }

    protected void sendAppendEntriesRequest(RaftAppendEntriesRequest request) {
        if (this.disposed) {
            logger.warn("{} tried to send {} after it is stopped", this.id, request);
            return;
        }
        this.raft.outboundEvents().appendEntriesRequest().onNext(request);
    }

    protected AbstractActor onAppendEntriesResponse(Consumer<RaftAppendEntriesResponse> listener) {
        this.addDisposable(this.raft.inboundEvents().appendEntriesResponse().subscribe(response -> {
            if (this.disposed) {
                logger.warn("{} received {} after it is stopped", this.id, response);
                return;
            }
            synchronized (this.oneMessage) {
                listener.accept(response);
            }
        }));
        return this;
    }

    protected void sendAppendEntriesResponse(RaftAppendEntriesResponse response) {
        if (this.disposed) {
            logger.warn("{} tried to send {} after it is stopped", this.id, response);
            return;
        }
        this.raft.outboundEvents().appendEntriesResponse().onNext(response);
    }

    @Override
    public boolean isDisposed() {
        return this.disposed || this.disposer.isDisposed();
    }

    @Override
    public void dispose() {
        if (this.disposed) {
            return;
        }
        this.disposed = true;
        this.disposer.dispose();
    }

}
