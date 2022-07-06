package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.mappings.Decoder;
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
import java.util.function.Consumer;

public class StorageSyncOperation {

    private static Logger logger = LoggerFactory.getLogger(StorageSyncOperation.class);

    public static Builder builder() {
        return new Builder();
    }

    private record Result(Integer commitIndex, Throwable e) {

    }

    private final Set<Integer> processedSequences = new HashSet<>();
    private final Set<String> cleanedStorageIds = new HashSet<>();


    private int commitIndex = -1;
    private int endSeq = -1;
    private UUID requestId;
    private Consumer<String> cleanLocalStorageConsumer;
    private Consumer<Message> processNotificationConsumer;
    private Decoder<Message, StorageSyncResponse> decoder;
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
            response = this.decoder.decode(message);
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
        for (var it = response.storageUpdateNotifications().entrySet().iterator(); it.hasNext(); ) {
            var entry = it.next();
            var storageId = entry.getKey();
            var updateNotification = entry.getValue();
            if (!cleanedStorageIds.contains(storageId)) {
                this.cleanLocalStorageConsumer.accept(storageId);
            }
            this.processNotificationConsumer.accept(updateNotification);
        }
        this.processedSequences.add(response.sequence());
        if (0 <= this.endSeq && this.endSeq == this.processedSequences.size() - 1) {
            this.promise.complete(new Result(
                    this.commitIndex,
                    null
            ));
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

        public Builder setDecoder(Decoder<Message, StorageSyncResponse> decoder) {
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
