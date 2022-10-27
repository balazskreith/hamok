package io.github.balazskreith.hamok.storagegrid.messages;

import io.github.balazskreith.hamok.common.KeyValuePair;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StorageOpSerDe<K, V> {

    private final Function<K, byte[]> keyEncoder;
    private final Function<byte[], K> keyDecoder;
    private final Function<V, byte[]> valueEncoder;
    private final Function<byte[], V> valueDecoder;

    public StorageOpSerDe(Function<K, byte[]> keyEncoder, Function<byte[], K> keyDecoder, Function<V, byte[]> valueEncoder, Function<byte[], V> valueDecoder) {
        this.keyEncoder = keyEncoder;
        this.keyDecoder = keyDecoder;
        this.valueEncoder = valueEncoder;
        this.valueDecoder = valueDecoder;
    }


    public Message serializeClearEntriesNotification(ClearEntriesNotification notification) {
        var result = new Message();
        result.type = MessageType.CLEAR_ENTRIES_NOTIFICATION.name();
        return result;
    }

    public ClearEntriesNotification deserializeClearEntriesNotification(Message message) {
        return new ClearEntriesNotification(message.sourceId);
    }

    public Message serializeClearEntriesRequest(ClearEntriesRequest request) {
        var result = new Message();
        result.type = MessageType.CLEAR_ENTRIES_REQUEST.name();
        result.requestId = request.requestId();
        return result;
    }

    public ClearEntriesRequest deserializeClearEntriesRequest(Message message) {
        return new ClearEntriesRequest(message.requestId, message.sourceId);
    }

    public Message serializeClearEntriesResponse(ClearEntriesResponse response) {
        var result = new Message();
        result.type = MessageType.CLEAR_ENTRIES_RESPONSE.name();
        result.destinationId = response.destinationEndpointId();
        result.requestId = response.requestId();
        return result;
    }

    public ClearEntriesResponse deserializeClearEntriesResponse(Message message) {
        return new ClearEntriesResponse(
                message.requestId,
                message.destinationId
        );
    }


    public Message serializeGetKeysRequest(GetKeysRequest request) {
        var result = new Message();
        result.type = MessageType.GET_KEYS_REQUEST.name();
        result.requestId = request.requestId();
        return result;
    }

    public GetKeysRequest deserializeGetKeysRequest(Message message) {
        return new GetKeysRequest(message.requestId, message.sourceId);
    }

    public Message serializeGetKeysResponse(GetKeysResponse<K> response) {
        var result = new Message();
        result.type = MessageType.GET_KEYS_RESPONSE.name();
        result.destinationId = response.destinationEndpointId();
        result.requestId = response.requestId();
        result.keys =  this.serializeKeys(response.keys());
        return result;
    }

    public GetKeysResponse<K> deserializeGetKeysResponse(Message message) {
        var keys  = this.deserializeKeys(message.keys);
        return new GetKeysResponse<>(
                message.requestId,
                keys,
                message.destinationId
        );
    }

    public Message serializeGetSizeRequest(GetSizeRequest request) {
        var result = new Message();
        result.type = MessageType.GET_SIZE_REQUEST.name();
        result.requestId = request.requestId();
        return result;
    }

    public GetSizeRequest deserializeGetSizeRequest(Message message) {
        return new GetSizeRequest(message.requestId, message.sourceId);
    }

    public Message serializeGetSizeResponse(GetSizeResponse response) {
        var result = new Message();
        result.type = MessageType.GET_SIZE_RESPONSE.name();
        result.destinationId = response.destinationEndpointId();
        result.requestId = response.requestId();
        result.storageSize = response.size();
        return result;
    }

    public GetSizeResponse deserializeGetSizeResponse(Message message) {
        return new GetSizeResponse(
                message.requestId,
                message.storageSize,
                message.destinationId
        );
    }

    public Message serializeGetEntriesRequest(GetEntriesRequest<K> request) {
        var result = new Message();
        result.type = MessageType.GET_ENTRIES_REQUEST.name();
        result.keys = this.serializeKeys(request.keys());
        result.requestId = request.requestId();
        return result;
    }

    public GetEntriesRequest<K> deserializeGetEntriesRequest(Message message) {
        var keys = this.deserializeKeys(message.keys);
        return new GetEntriesRequest<>(keys, message.requestId, message.sourceId);
    }

    public Message serializeGetEntriesResponse(GetEntriesResponse<K, V> response) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(response.foundEntries());
        result.type = MessageType.GET_ENTRIES_RESPONSE.name();
        result.destinationId = response.destinationEndpointId();
        result.requestId = response.requestId();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        return result;
    }

    public GetEntriesResponse<K, V> deserializeGetEntriesResponse(Message message) {
        var foundEntries  = this.deserializeEntries(message.keys, message.values);
        return new GetEntriesResponse<>(
                message.requestId,
                foundEntries,
                message.destinationId
        );
    }

    public Message serializeDeleteEntriesNotification(DeleteEntriesNotification<K> notification) {
        var result = new Message();
        result.type = MessageType.DELETE_ENTRIES_NOTIFICATION.name();
        result.keys = this.serializeKeys(notification.keys());
        result.sourceId = notification.sourceEndpointId();
        result.destinationId = notification.destinationEndpointId();
        return result;
    }

    public DeleteEntriesNotification<K> deserializeDeleteEntriesNotification(Message message) {
        var keys = this.deserializeKeys(message.keys);
        var result = new DeleteEntriesNotification<K>(message.sourceId, keys, message.destinationId);
        return result;
    }

    public Message serializeDeleteEntriesRequest(DeleteEntriesRequest<K> request) {
        var result = new Message();
        result.type = MessageType.DELETE_ENTRIES_REQUEST.name();
        result.keys = this.serializeKeys(request.keys());
        result.requestId = request.requestId();
        return result;
    }

    public DeleteEntriesRequest<K> deserializeDeleteEntriesRequest(Message message) {
        var keys = this.deserializeKeys(message.keys);
        var result = new DeleteEntriesRequest<K>(message.requestId, keys, message.sourceId);
        return result;
    }

    public Message serializeDeleteEntriesResponse(DeleteEntriesResponse<K> response) {
        var result = new Message();
        result.type = MessageType.DELETE_ENTRIES_RESPONSE.name();
        result.keys = this.serializeKeys(response.deletedKeys());
        result.requestId = response.requestId();
        result.destinationId = response.destinationEndpointId();
        return result;
    }

    public DeleteEntriesResponse<K> deserializeDeleteEntriesResponse(Message message) {
        var deletedKeys = this.deserializeKeys(message.keys);
        return new DeleteEntriesResponse<K>(message.requestId, deletedKeys, message.sourceId);
    }

    public Message serializeRemoveEntriesRequest(RemoveEntriesRequest<K> request) {
        var result = new Message();
        result.type = MessageType.REMOVE_ENTRIES_REQUEST.name();
        result.keys = this.serializeKeys(request.keys());
        result.requestId = request.requestId();
        return result;
    }

    public RemoveEntriesRequest<K> deserializeRemoveEntriesRequest(Message message) {
        var keys = this.deserializeKeys(message.keys);
        var result = new RemoveEntriesRequest<K>(message.requestId, keys, message.sourceId);
        return result;
    }

    public Message serializeRemoveEntriesResponse(RemoveEntriesResponse<K, V> response) {
        var result = new Message();
        result.type = MessageType.REMOVE_ENTRIES_RESPONSE.name();
        var entries = this.serializeEntries(response.removedEntries());
        result.keys = entries.getKey();
        result.values = entries.getValue();
        result.requestId = response.requestId();
        result.destinationId = response.destinationEndpointId();
        return result;
    }

    public RemoveEntriesResponse<K, V> deserializeRemoveEntriesResponse(Message message) {
        var removedEntries = this.deserializeEntries(message.keys, message.values);
        return new RemoveEntriesResponse<K, V>(message.requestId, removedEntries, message.sourceId);
    }



    public Message serializeEvictEntriesNotification(EvictEntriesNotification<K> notification) {
        var result = new Message();
        result.type = MessageType.EVICT_ENTRIES_NOTIFICATION.name();
        result.keys = this.serializeKeys(notification.keys());
        result.sourceId = notification.sourceEndpointId();
        result.destinationId = notification.destinationEndpointId();
        return result;
    }

    public EvictEntriesNotification<K> deserializeEvictEntriesNotification(Message message) {
        var keys = this.deserializeKeys(message.keys);
        var result = new EvictEntriesNotification<K>(message.sourceId, keys, message.destinationId);
        return result;
    }


    public Message serializeEvictEntriesRequest(EvictEntriesRequest<K> request) {
        var result = new Message();
        result.type = MessageType.EVICT_ENTRIES_REQUEST.name();
        result.keys = this.serializeKeys(request.keys());
        result.requestId = request.requestId();
        return result;
    }

    public EvictEntriesRequest<K> deserializeEvictEntriesRequest(Message message) {
        var keys = this.deserializeKeys(message.keys);
        var result = new EvictEntriesRequest<K>(message.requestId, keys, message.sourceId);
        return result;
    }

    public Message serializeEvictEntriesResponse(EvictEntriesResponse<K> response) {
        var result = new Message();
        result.type = MessageType.EVICT_ENTRIES_RESPONSE.name();
        result.requestId = response.requestId();
        result.destinationId = response.destinationEndpointId();
        return result;
    }

    public EvictEntriesResponse<K> deserializeEvictEntriesResponse(Message message) {
        return new EvictEntriesResponse<K>(message.requestId, message.sourceId);
    }

    public Message serializeUpdateEntriesNotification(UpdateEntriesNotification<K, V> notification) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(notification.entries());
        result.type = MessageType.UPDATE_ENTRIES_NOTIFICATION.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        result.destinationId = notification.destinationEndpointId();
        return result;
    }

    public UpdateEntriesNotification<K, V> deserializeUpdateEntriesNotification(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        var result = new UpdateEntriesNotification<K, V>(entries, message.sourceId, message.destinationId);
        return result;
    }

    public Message serializeUpdateEntriesRequest(UpdateEntriesRequest<K, V> request) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(request.entries());
        result.type = MessageType.UPDATE_ENTRIES_REQUEST.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        result.requestId = request.requestId();
        return result;
    }

    public UpdateEntriesRequest<K, V> deserializeUpdateEntriesRequest(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        return new UpdateEntriesRequest<>(
                message.requestId,
                entries,
                message.sourceId
        );
    }

    public Message serializeUpdateEntriesResponse(UpdateEntriesResponse<K, V> response) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(response.entries());
        result.type = MessageType.UPDATE_ENTRIES_RESPONSE.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        result.requestId = response.requestId();
        result.destinationId = response.destinationEndpointId();
        return result;
    }

    public UpdateEntriesResponse<K, V> deserializeUpdateEntriesResponse(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        return new UpdateEntriesResponse<>(
                message.requestId,
                entries,
                message.destinationId
        );
    }


    public Message serializeInsertEntriesRequest(InsertEntriesRequest<K, V> request) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(request.entries());
        result.type = MessageType.INSERT_ENTRIES_REQUEST.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        result.requestId = request.requestId();
        return result;
    }

    public InsertEntriesRequest<K, V> deserializeInsertEntriesRequest(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        return new InsertEntriesRequest<>(
                message.requestId,
                entries,
                message.sourceId
        );
    }

    public Message serializeInsertEntriesResponse(InsertEntriesResponse<K, V> response) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(response.existingEntries());
        result.type = MessageType.INSERT_ENTRIES_RESPONSE.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        result.requestId = response.requestId();
        result.destinationId = response.destinationEndpointId();
        return result;
    }

    public Message serializeInsertEntriesNotification(InsertEntriesNotification<K, V> notification) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(notification.entries());
        result.type = MessageType.INSERT_ENTRIES_NOTIFICATION.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        return result;
    }

    public InsertEntriesNotification<K, V> deserializeInsertEntriesNotification(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        return new InsertEntriesNotification<K, V>(
                entries,
                message.sourceId,
                message.destinationId
        );
    }

    public InsertEntriesResponse<K, V> deserializeInsertEntriesResponse(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        return new InsertEntriesResponse<>(
                message.requestId,
                entries,
                message.destinationId
        );
    }

    public Message serializeRemoveEntriesNotification(RemoveEntriesNotification<K, V> notification) {
        var result = new Message();
        var serializedEntries = this.serializeEntries(notification.entries());
        result.type = MessageType.REMOVE_ENTRIES_NOTIFICATION.name();
        result.keys = serializedEntries.getKey();
        result.values = serializedEntries.getValue();
        return result;
    }

    public RemoveEntriesNotification<K, V> deserializeRemoveEntriesNotification(Message message) {
        var entries = this.deserializeEntries(message.keys, message.values);
        return new RemoveEntriesNotification<>(
                entries,
                message.sourceId
        );
    }

    public KeyValuePair<List<byte[]>, List<byte[]>> serializeEntries(Map<K, V> entries) {
        if (entries == null || entries.size() < 1) {
            return KeyValuePair.of(Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        }
        var keys = new LinkedList<byte[]>();
        var values = new LinkedList<byte[]>();
        entries.forEach((entryKey, entryValue) -> {
            var key = this.keyEncoder.apply(entryKey);
            var value = this.valueEncoder.apply(entryValue);
            if (entryKey == null || entryValue == null) {
                return;
            }
            keys.add(key);
            values.add(value);
        });
        return KeyValuePair.of(keys, values);
    }

    public Map<K, V> deserializeEntries(List<byte[]> keys, List<byte[]> values) {
        var result = new HashMap<K, V>();
        var keyIt = keys.iterator();
        var valuesIt = values.iterator();
        while (keyIt.hasNext() && valuesIt.hasNext()) {
            var foundKey = keyIt.next();
            var foundValue = valuesIt.next();
            var entryKey = this.keyDecoder.apply(foundKey);
            var entryValue = this.valueDecoder.apply(foundValue);
            if (entryKey != null && entryValue != null) {
                result.put(entryKey, entryValue);
            }
        }
        return result;
    }

    public List<byte[]> serializeKeys(Collection<K> keys) {
        if (keys == null || keys.size() < 1) {
            return Collections.EMPTY_LIST;
        }
        return keys.stream()
                .map(this.keyEncoder)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public Set<K> deserializeKeys(List<byte[]> keys) {
        if (keys == null || keys.size() < 1) {
            return Collections.EMPTY_SET;
        }
        return keys.stream()
                .map(this.keyDecoder)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }


}
