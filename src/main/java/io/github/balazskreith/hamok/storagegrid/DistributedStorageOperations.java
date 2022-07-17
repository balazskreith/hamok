package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.storagegrid.messages.UpdateEntriesNotification;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

class DistributedStorageOperations {
    public static<U, R> Builder<U, R> builder() {
        return new Builder<U, R>();
    }

    private List<MessageAcceptor> acceptors;
    private String storageId;
    private String storageClassSimpleName;
    private Function<UUID, Message> getAllKeysUpdateNotification;
    private Runnable localClear;


    private DistributedStorageOperations() {

    }

    public String getStorageId() {
        return this.storageId;
    }

    public String getStorageClassSimpleName() {
        return this.storageClassSimpleName;
    }

    public void storageLocalClear() {
        this.localClear.run();
    }

    public void accept(Message message) {
        if (message == null) {
            return;
        }
        for (var messageAcceptor : this.acceptors) {
            if (message.protocol != null && !message.protocol.equals(messageAcceptor.protocol)) {
                continue;
            }
            messageAcceptor.acceptor.accept(message);
        }
    }

    public Message getAllKeysUpdateNotification(UUID destinationEndpointId) {
        return this.getAllKeysUpdateNotification.apply(destinationEndpointId);
    }

    private record MessageAcceptor(Consumer<Message> acceptor, String protocol) {

    }

    public static class Builder<K, V> {
        private List<StorageEndpoint<K, V>> endpoints = Collections.synchronizedList(new LinkedList<>());
        private DistributedStorage<K, V> storage;

        public Builder<K, V> withStorage(DistributedStorage<K, V> storage) {
            this.storage = storage;
            return this;
        }

        public Builder<K, V> withEndpoint(StorageEndpoint<K, V> endpoint) {
            this.endpoints.add(endpoint);
            return this;
        }

        public DistributedStorageOperations build() {
            var result = new DistributedStorageOperations();
            result.storageClassSimpleName = this.storage.getClass().getSimpleName();
            result.storageId = this.storage.getId();
            result.localClear = this.storage::localClear;
            result.acceptors = this.endpoints.stream()
                    .<MessageAcceptor>map(endpoint -> new MessageAcceptor(
                            message -> endpoint.receive(message),
                            endpoint.protocol
                    ))
                    .collect(Collectors.toList());
            result.getAllKeysUpdateNotification = destinationId -> {
                var keys = this.storage.keys();
                var entries = this.storage.getAll(keys);
                var notification = new UpdateEntriesNotification<K, V>(entries, null, destinationId);
                var endpoint = this.endpoints.get(0);
                return endpoint.messageSerDe.serializeUpdateEntriesNotification(notification);
            };
            result.storageId = this.storage.getId();
            return result;
        }
    }

}
