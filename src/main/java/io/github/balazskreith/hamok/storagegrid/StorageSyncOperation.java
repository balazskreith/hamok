package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.storagegrid.messages.MessageType;
import io.github.balazskreith.hamok.storagegrid.messages.StorageSyncResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;

public class StorageSyncOperation {

    private static Logger logger = LoggerFactory.getLogger(StorageSyncOperation.class);

    public static Builder builder() {
        return new Builder();
    }

    private record Result(Integer commitIndex, Throwable e) {

    }

//    record Notification(String storageId, int order, Message notification) {
//
//    }

    private final Set<Integer> processedSequences = new HashSet<>();
    private final Set<String> cleanedStorageIds = new HashSet<>();

//    private Map<String, List<Notification>> notifications = new HashMap<>();

    private int commitIndex = -1;
    private int endSeq = -1;
    private UUID requestId;
    private Consumer<String> cleanLocalStorageConsumer;
    private Consumer<Message> processNotificationConsumer;
    private Function<Message, StorageSyncResponse> decoder;
    private CompletableFuture<Result> promise = new CompletableFuture<>();

    private StorageSyncOperation() {

    }

    public synchronized void process(Message message) {
        if (!MessageType.STORAGE_SYNC_RESPONSE.equals(MessageType.valueOf(message.type))) {
            logger.warn("StorageSyncOperation received not a storage sync response");
            return;
        }
        if (UuidTools.notEquals(message.requestId, this.requestId)) {
            logger.warn("RequestId for Storage Sync Request is not equal. Storage sync operation requestd: {}, message: {}", this.requestId, message);
            return;
        }
        StorageSyncResponse response;
        try {
            response = this.decoder.apply(message);
        } catch (Throwable e) {
            logger.warn("Cannot decode message {}", message, e);
            return;
        }
        if (response.success() == false) {
            logger.warn("Response is not successful");
            return;
        }
        if (this.commitIndex < 0) {
            this.commitIndex = response.commitIndex();
        }
        if (Boolean.TRUE.equals(response.lastMessage())) {
            this.endSeq = response.sequence();
        }
//        logger.info("Received {}", response);
        for (var it = response.storageUpdateNotifications().entrySet().iterator(); it.hasNext(); ) {
            var entry = it.next();
            var storageId = entry.getKey();
            var updateNotification = entry.getValue();
            if (!cleanedStorageIds.contains(storageId)) {
                this.cleanLocalStorageConsumer.accept(storageId);
                cleanedStorageIds.add(storageId);
                logger.debug("Storage {} is cleaned", storageId);
            }
            this.processNotificationConsumer.accept(updateNotification);
            logger.trace("Update storage {} by {}", storageId, updateNotification.type);
        }
        this.processedSequences.add(response.sequence());

        var ready = false;
        if (this.endSeq == 0) {
            ready = true;
        } else {
            ready = this.endSeq == this.processedSequences.size();
        }
        logger.trace("Storage sync is building, {}, ready: {}", this, ready);
        if (ready) {
            this.promise.complete(new Result(
                    this.commitIndex,
                    null
            ));
        }
    }

    @Override
    public String toString() {
        return String.format("processed messages: %d, endseq: %d, commitIndex: %d", this.processedSequences.size(), this.endSeq, this.commitIndex);
    }

    public void await() {
        if (this.promise.isDone() || this.promise.isCancelled()) {
            return;
        }
        try {
            this.promise.get();
        } catch (InterruptedException e) {
            logger.error("Error occurred while awaitng sync storage operation", e);
        } catch (ExecutionException e) {
            logger.error("Error occurred while awaitng sync storage operation", e);
        }
    }

    public StorageSyncOperation onCompleted(Consumer<Integer> onCompletedListener) {
        this.promise.thenAccept(result -> {
            if (result.commitIndex != null) {
                onCompletedListener.accept(result.commitIndex);
            }
        });
        return this;
    }

    public StorageSyncOperation onFailed(Consumer<Throwable> onFailedListener) {
        this.promise.thenAccept(result -> {
            if (result.e != null) {
                onFailedListener.accept(result.e);
            }
        });
        return this;
    }

    public static class Builder {
        private StorageSyncOperation result = new StorageSyncOperation();
        private Builder() {

        }

        public Builder setRequestId(UUID value) {
            this.result.requestId = value;
            return this;
        }

        public Builder setCleanLocalStorageConsumer(Consumer<String> cleanLocalStorageConsumer) {
            this.result.cleanLocalStorageConsumer = cleanLocalStorageConsumer;
            return this;
        }

        public Builder setProcessNotificationConsumer(Consumer<Message> processNotificationConsumer) {
            this.result.processNotificationConsumer = processNotificationConsumer;
            return this;
        }

        public Builder setDecoder(Function<Message, StorageSyncResponse> decoder) {
            this.result.decoder = decoder;
            return this;
        }

        public StorageSyncOperation build() {
            Objects.requireNonNull(this.result.requestId, "requestId must be set");
            Objects.requireNonNull(this.result.cleanLocalStorageConsumer, "clean local storage consumer must be set");
            Objects.requireNonNull(this.result.decoder, "Decoder must be defined");
            Objects.requireNonNull(this.result.processNotificationConsumer, "notificationConsumer must be set");
            return this.result;
        }
    }
}
