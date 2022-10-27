package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PendingRequest implements Consumer<Models.Message> {
    private static final Logger logger = LoggerFactory.getLogger(PendingRequest.class);

    private PendingRequest() {
    }

    public static Builder builder() {
        return new Builder();
    }

    private UUID id = null;
    private int timeoutInMs = 0;
    private int receivedResponse = 0;
    private int neededResponses = -1;
    private boolean throwTimeoutException = true;
    private List<Models.Message> responses = new LinkedList<>();
    private Set<UUID> pendingEndpointIds = new HashSet<>();
    private CompletableFuture<Void> completableFuture = new CompletableFuture<>();

    private volatile boolean timedOut = false;

    public UUID getId() {
        return this.id;
    }

    @Override
    public void accept(Models.Message message) {
        UUID sourceEndpointId = Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId);
        if (sourceEndpointId == null) {
            logger.warn("No source id is assigned for message: {}", message);
            return;
        }
        var requestId = Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId);
        boolean completed = false;
        try {
            synchronized (this) {
                var pendingBefore = this.pendingEndpointIds.size();
                if (!this.pendingEndpointIds.remove(sourceEndpointId) && this.neededResponses < 1) {
                    logger.debug("Source endpoint {} is not found in pending ids of request {}", message.getSourceId(), requestId);
                    // fail-safe double checking to complete every pending request which has to be completed
                    completed = pendingBefore == 0;
                    return;
                }
                var pendingAfter = this.pendingEndpointIds.size();
                this.responses.add(message);
                ++this.receivedResponse;
                logger.trace("{} pending before {}, pending after: {}", this.id.toString().substring(0, 8), pendingBefore, pendingAfter);
                completed = (pendingBefore == 1 && pendingAfter == 0) || (0 < this.neededResponses && this.receivedResponse <= this.neededResponses);
            }
        } finally {
            if (completed) {
                this.completableFuture.complete(null);
            }
        }
    }

    public void onCompleted(Runnable action) {
        this.completableFuture.thenRun(action);
    }

    public void removeEndpointId(UUID endpointId) {
        synchronized (this) {
            this.pendingEndpointIds.remove(endpointId);
            if (this.pendingEndpointIds.size() < 1 && this.neededResponses < 0) {
                this.completableFuture.complete(null);
            }
        }
    }

    public void addEndpointId(UUID endpointId) {
        synchronized (this) {
            this.pendingEndpointIds.add(endpointId);
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        return this.completableFuture.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled() {
        return this.completableFuture.isCancelled();
    }

    public boolean isDone() {
        return this.completableFuture.isDone();
    }

    public List<Models.Message> get()  throws ExecutionException, InterruptedException, TimeoutException  {
        synchronized (this) {
            if (this.pendingEndpointIds.size() < 1 && this.neededResponses < 0) {
                // if the application receive the response faster than reaching the get() point, then we are already done
//                logger.warn("Resolved by not even tried {} {}", this.pendingEndpointIds.size(), this.neededResponses);
                return Collections.unmodifiableList(this.responses);
            }
        }
        try {
            if (0 < this.timeoutInMs) {
                this.completableFuture.get(this.timeoutInMs, TimeUnit.MILLISECONDS);
            } else {
                this.completableFuture.get();
            }
        } catch(TimeoutException ex) {
            this.timedOut = true;
            if (this.throwTimeoutException) {
                logger.warn("Failed pending request {} due to exception {}", this, ex.getClass().getSimpleName());
                throw ex;
            }
            synchronized (this) {
                logger.warn("Pending request is timed out and resolved with missing responses. {}", this);
                return Collections.unmodifiableList(this.responses);
            }
        } catch (Exception ex) {
            logger.warn("Error occurred while waiting for request {}", this, ex);
            throw ex;
        }

        synchronized (this) {
            logger.trace("Pending request is resolved by responses {}", this.responses);
            return Collections.unmodifiableList(this.responses);
        }
    }

    public Set<UUID> getRemainingEndpointIds(){
        synchronized (this) {
            return Set.copyOf(this.pendingEndpointIds);
        }
    }

    public boolean isTimedOut() {
        return this.timedOut;
    }

    @Override
    public String toString() {
        var remainingEndpointIds = String.join(", ", this.pendingEndpointIds.stream()
                .map(Object::toString)
                .collect(Collectors.toList()));

        var acceptedEndpointIds = String.join(", ", this.responses.stream()
//                .map(m -> m.sourceId)
                .map(m -> Utils.supplyStringToUuidIfTrue(m.hasSourceId(), m::getSourceId))
                .filter(Objects::nonNull)
                .map(UUID::toString)
                .collect(Collectors.toList()));

        return String.format("Pending request id: %s, received responses: %d, remaining endpoints: %s, accepted endpoints: %s timeout: %d",
                this.id,
                this.receivedResponse,
                remainingEndpointIds,
                acceptedEndpointIds,
                this.timeoutInMs
        );
    }

    public static class Builder {
        private PendingRequest pendingRequest = new PendingRequest();
        private Consumer<PendingRequest> onBuiltListener = r -> {};
        Builder() {

        }

        public Builder withPendingEndpoints(Set<UUID> endpointIds) {
            this.pendingRequest.pendingEndpointIds.addAll(endpointIds);
            return this;
        }

        public Builder withTimeoutInMs(int value) {
            this.pendingRequest.timeoutInMs = value;
            return this;
        }

        public Builder withNeededResponse(int value) {
            this.pendingRequest.neededResponses = value;
            return this;
        }

        public Builder withThrowingTimeoutException(boolean value) {
            this.pendingRequest.throwTimeoutException = value;
            return this;
        }

        public Builder withRequestId(UUID requestId) {
            this.pendingRequest.id = requestId;
            return this;
        }

        Builder onBuilt(Consumer<PendingRequest> onBuiltListener) {
            this.onBuiltListener = onBuiltListener;
            return this;
        }

        public PendingRequest build() {
            Objects.requireNonNull(this.pendingRequest.id, "Pending request must have an id");
            try {
                return this.pendingRequest;
            } finally {
                this.onBuiltListener.accept(this.pendingRequest);
            }
        }
    }
}
