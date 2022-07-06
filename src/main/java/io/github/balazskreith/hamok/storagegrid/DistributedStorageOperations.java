package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import io.github.balazskreith.hamok.storagegrid.messages.UpdateEntriesNotification;

import java.util.*;
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
    private Function<UUID, Iterator<Message>> getAllKeysUpdateNotification;
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

    public Iterator<Message> getAllKeysUpdateNotification(UUID destinationEndpointId) {
        return this.getAllKeysUpdateNotification.apply(destinationEndpointId);
    }

    private record MessageAcceptor(Consumer<Message> acceptor, String protocol) {

    }

    public static class Builder<K, V> {
        private List<StorageEndpoint<K, V>> endpoints = Collections.synchronizedList(new LinkedList<>());
        private DistributedStorage<K, V> storage;
        private int maxMessageEntries = 10000000;

        public Builder<K, V> withStorage(DistributedStorage<K, V> storage) {
            this.storage = storage;
            return this;
        }

        public Builder<K, V> withEndpoint(StorageEndpoint<K, V> endpoint) {
            this.endpoints.add(endpoint);
            return this;
        }

        public Builder<K, V> withMaxMessageEntries(int maxMessageEntries) {
            this.maxMessageEntries = maxMessageEntries;
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
                var it = keys.iterator();
                var endpoint = this.endpoints.get(0);
                return new Iterator<Message>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public Message next() {
                        var batchedKeys = new HashSet<K>();
                        for (int i = 0; it.hasNext() && i < maxMessageEntries; ++i) {
                            batchedKeys.add(it.next());
                        }
                        var entries = storage.getAll(batchedKeys);
                        var notification = new UpdateEntriesNotification<K, V>(entries, null, destinationId);
                        var message = endpoint.messageSerDe.serializeUpdateEntriesNotification(notification);
                        message.storageId = storage.getId();
                        message.protocol = endpoint.protocol;
                        return message;
                    }
                };
            };
            result.storageId = this.storage.getId();
            return result;
        }
    }

}
