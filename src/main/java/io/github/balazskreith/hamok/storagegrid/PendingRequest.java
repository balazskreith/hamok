package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.JsonUtils;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

public class PendingRequest implements Consumer<Message> {
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
    private List<Message> responses = new LinkedList<>();
    private Set<UUID> pendingEndpointIds = new HashSet<>();
    private CompletableFuture<Void> completableFuture = new CompletableFuture<>();

    public UUID getId() {
        return this.id;
    }

    @Override
    public void accept(Message message) {
        if (message.sourceId == null) {
            logger.warn("No source id is assigned for message: {}", JsonUtils.objectToString(message));
            return;
        }
        boolean completed = false;
        try {
            synchronized (this) {
                var pendingBefore = this.pendingEndpointIds.size();
                if (!this.pendingEndpointIds.remove(message.sourceId)) {
                    logger.warn("Source endpoint {} is not found in pending ids of request {}", message.sourceId, message.requestId);
                    completed = pendingBefore == 0;
                }
                var pendingAfter = this.pendingEndpointIds.size();
                this.responses.add(message);
                ++this.receivedResponse;
//                logger.warn("{} pending before {}, pending after: {}", this.id.toString().substring(0, 8), pendingBefore, pendingAfter);
                completed = (pendingBefore == 1 && pendingAfter == 0) || (0 < this.neededResponses && this.receivedResponse <= this.neededResponses);
            }
        } finally {
            if (completed) {
                this.completableFuture.complete(null);
            }
        }

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

    public List<Message> get()  throws ExecutionException, InterruptedException, TimeoutException  {
        synchronized (this) {
            if (this.pendingEndpointIds.size() < 1 && this.neededResponses < 0) {
                // if the application receive the response faster than reaching the get() point, then we are already done
                return Collections.unmodifiableList(this.responses);
            }
        }
        try {
            if (0 < this.timeoutInMs) {
                this.completableFuture.get(this.timeoutInMs, TimeUnit.MILLISECONDS);
            } else {
                this.completableFuture.get();
            }
        } catch (Exception ex) {
            logger.warn("Failed pending request {} due to exception {}", this, ex.getClass().getSimpleName());
            throw ex;
        }

        synchronized (this) {
            logger.warn("Pending request is resolved by responses {}", JsonUtils.objectToString(this.responses));
            return Collections.unmodifiableList(this.responses);
        }
    }

    @Override
    public String toString() {
        var remainingEndpoints = JsonUtils.objectToString(this.pendingEndpointIds);
        return String.format("Pending request id: %s, received responses: %d, remaining endpoints: %s, timeout: %d",
                this.id,
                this.receivedResponse,
                remainingEndpoints,
                this.timeoutInMs
        );
    }

    public static class Builder {
        private PendingRequest pendingRequest = new PendingRequest();
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

        public Builder withRequestId(UUID requestId) {
            this.pendingRequest.id = requestId;
            return this;
        }

        public PendingRequest build() {
            Objects.requireNonNull(this.pendingRequest.id, "Pending request must have an id");
            return this.pendingRequest;
        }


    }
}
