package io.github.balazskreith.vstorage.storagegrid.messages;

import io.github.balazskreith.vstorage.common.KeyValuePair;
import io.github.balazskreith.vstorage.mappings.Codec;
import io.github.balazskreith.vstorage.mappings.Mapper;

import java.util.*;
import java.util.stream.Collectors;

public class StorageOpSerDe<K, V> {
    private final Codec<K, String> keyCodec;
    private final Codec<V, String> valueCodec;

    public StorageOpSerDe(Codec<K, String> keyCodec, Codec<V, String> valueCodec) {
        this.keyCodec = keyCodec;
        this.valueCodec = valueCodec;
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

    public Message serializeEvictEntriesNotification(EvictEntriesNotification<K> notification) {
        var result = new Message();
        result.type = MessageType.EVICT_ENTRIES_NOTIFICATION.name();
        result.keys = this.serializeKeys(notification.keys());
        result.destinationId = notification.destinationEndpointId();
        return result;
    }

    public EvictEntriesNotification<K> deserializeEvictEntriesNotification(Message message) {
        var keys = this.deserializeKeys(message.keys);
        return new EvictEntriesNotification<>(
                keys,
                message.sourceId,
                message.destinationId
        );
    }

    public KeyValuePair<List<String>, List<String>> serializeEntries(Map<K, V> entries) {
        if (entries == null || entries.size() < 1) {
            return KeyValuePair.of(Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        }
        var keyEncoder = Mapper.create(this.keyCodec::encode);
        var valueEncoder = Mapper.create(this.valueCodec::encode);
        var keys = new LinkedList<String>();
        var values = new LinkedList<String>();
        entries.forEach((entryKey, entryValue) -> {
            var key = keyEncoder.map(entryKey);
            var value = valueEncoder.map(entryValue);
            if (entryKey == null || entryValue == null) {
                return;
            }
            keys.add(key);
            values.add(value);
        });
        return KeyValuePair.of(keys, values);
    }

    public Map<K, V> deserializeEntries(List<String> keys, List<String> values) {
        var keyDecoder = Mapper.create(this.keyCodec::decode);
        var valueDecoder = Mapper.create(this.valueCodec::decode);
        var result = new HashMap<K, V>();
        for (int i = 0, c = Math.min(keys.size(), values.size()); i < c; ++i) {
            var foundKey = keys.get(i);
            var foundValue = values.get(i);
            var entryKey = keyDecoder.map(foundKey);
            var entryValue = valueDecoder.map(foundValue);
            if (entryKey != null && entryValue != null) {
                result.put(entryKey, entryValue);
            }
        }
        return result;
    }

    public List<String> serializeKeys(Collection<K> keys) {
        if (keys == null || keys.size() < 1) {
            return Collections.EMPTY_LIST;
        }
        var keyEncoder = Mapper.create(this.keyCodec::encode);
        return keys.stream()
                .map(keyEncoder::map)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public Set<K> deserializeKeys(List<String> keys) {
        if (keys == null || keys.size() < 1) {
            return Collections.EMPTY_SET;
        }
        var keyDecoder = Mapper.create(this.keyCodec::decode);
        return keys.stream()
                .map(keyDecoder::map)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }
}
