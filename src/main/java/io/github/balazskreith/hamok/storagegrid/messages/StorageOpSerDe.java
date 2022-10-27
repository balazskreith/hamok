package io.github.balazskreith.hamok.storagegrid.messages;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.KeyValuePair;
import io.github.balazskreith.hamok.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StorageOpSerDe<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(StorageOpSerDe.class);

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


    public Models.Message.Builder serializeClearEntriesNotification(ClearEntriesNotification notification) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.CLEAR_ENTRIES_NOTIFICATION.name())
                ;
        Utils.relayUuidToStringIfNotNull(notification::sourceEndpointId, result::setSourceId);
        return result;
    }

    public ClearEntriesNotification deserializeClearEntriesNotification(Models.Message message) {
        return new ClearEntriesNotification(
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeClearEntriesRequest(ClearEntriesRequest request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.CLEAR_ENTRIES_REQUEST.name())
                ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public ClearEntriesRequest deserializeClearEntriesRequest(Models.Message message) {
        return new ClearEntriesRequest(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeClearEntriesResponse(ClearEntriesResponse response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.CLEAR_ENTRIES_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public ClearEntriesResponse deserializeClearEntriesResponse(Models.Message message) {
        return new ClearEntriesResponse(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }


    public Models.Message.Builder serializeGetKeysRequest(GetKeysRequest request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.GET_KEYS_REQUEST.name())
                ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public GetKeysRequest deserializeGetKeysRequest(Models.Message message) {
        return new GetKeysRequest(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeGetKeysResponse(GetKeysResponse<K> response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.GET_KEYS_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayIfNotNull(() -> this.serializeKeys(response.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public GetKeysResponse<K> deserializeGetKeysResponse(Models.Message message) {
        var keys  = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new GetKeysResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public Models.Message.Builder serializeGetSizeRequest(GetSizeRequest request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.GET_SIZE_REQUEST.name())
                ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public GetSizeRequest deserializeGetSizeRequest(Models.Message message) {
        return new GetSizeRequest(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeGetSizeResponse(GetSizeResponse response) {
        var result = Models.Message.newBuilder()
                .setType( MessageType.GET_SIZE_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayIfNotNull(response::size, result::setStorageSize);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public GetSizeResponse deserializeGetSizeResponse(Models.Message message) {
        return new GetSizeResponse(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyIfTrue(message.hasStorageSize(), message::getStorageSize),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );

    }

    public Models.Message.Builder serializeGetEntriesRequest(GetEntriesRequest<K> request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.GET_ENTRIES_REQUEST.name())
                ;
        Utils.relayIfNotNull(() -> this.serializeKeys(request.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public GetEntriesRequest<K> deserializeGetEntriesRequest(Models.Message message) {
        var keys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new GetEntriesRequest<>(
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeGetEntriesResponse(GetEntriesResponse<K, V> response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.GET_ENTRIES_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        var serializedEntries = this.serializeEntries(response.foundEntries());
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public GetEntriesResponse<K, V> deserializeGetEntriesResponse(Models.Message message) {
        var foundEntries  = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new GetEntriesResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                foundEntries,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public Models.Message.Builder serializeDeleteEntriesNotification(DeleteEntriesNotification<K> notification) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.DELETE_ENTRIES_NOTIFICATION.name())
                ;
        Utils.relayUuidToStringIfNotNull(notification::sourceEndpointId, result::setSourceId);
        Utils.relayIfNotNull(() -> this.serializeKeys(notification.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(notification::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public DeleteEntriesNotification<K> deserializeDeleteEntriesNotification(Models.Message message) {
        var keys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new DeleteEntriesNotification<>(
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId),
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public Models.Message.Builder serializeDeleteEntriesRequest(DeleteEntriesRequest<K> request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.DELETE_ENTRIES_REQUEST.name())
                ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayIfNotNull(() -> this.serializeKeys(request.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public DeleteEntriesRequest<K> deserializeDeleteEntriesRequest(Models.Message message) {
        var keys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new DeleteEntriesRequest<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeDeleteEntriesResponse(DeleteEntriesResponse<K> response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.DELETE_ENTRIES_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayIfNotNull(() -> this.serializeKeys(response.deletedKeys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public DeleteEntriesResponse<K> deserializeDeleteEntriesResponse(Models.Message message) {
        var deletedKeys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new DeleteEntriesResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                deletedKeys,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );

    }

    public Models.Message.Builder serializeRemoveEntriesRequest(RemoveEntriesRequest<K> request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.REMOVE_ENTRIES_REQUEST.name())
                ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayIfNotNull(() -> this.serializeKeys(request.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public RemoveEntriesRequest<K> deserializeRemoveEntriesRequest(Models.Message message) {
        var keys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new RemoveEntriesRequest<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeRemoveEntriesResponse(RemoveEntriesResponse<K, V> response) {
        var result = Models.Message.newBuilder().setType(MessageType.REMOVE_ENTRIES_RESPONSE.name());
        var entries = this.serializeEntries(response.removedEntries());
        Utils.relayIfNotNull(entries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(entries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public RemoveEntriesResponse<K, V> deserializeRemoveEntriesResponse(Models.Message message) {
        var removedEntries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new RemoveEntriesResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                removedEntries,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }


    public Models.Message.Builder serializeEvictEntriesNotification(EvictEntriesNotification<K> notification) {
        var result = Models.Message.newBuilder().setType(MessageType.EVICT_ENTRIES_NOTIFICATION.name());
        Utils.relayIfNotNull(() -> this.serializeKeys(notification.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(notification::sourceEndpointId, result::setSourceId);
        Utils.relayUuidToStringIfNotNull(notification::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public EvictEntriesNotification<K> deserializeEvictEntriesNotification(Models.Message message) {
        var keys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new EvictEntriesNotification<>(
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId),
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }


    public Models.Message.Builder serializeEvictEntriesRequest(EvictEntriesRequest<K> request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.EVICT_ENTRIES_REQUEST.name())
                ;
        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayIfNotNull(() -> this.serializeKeys(request.keys()), result::addAllKeys);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public EvictEntriesRequest<K> deserializeEvictEntriesRequest(Models.Message message) {
        var keys = this.deserializeKeys(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList)
        );
        return new EvictEntriesRequest<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                keys,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeEvictEntriesResponse(EvictEntriesResponse<K> response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.EVICT_ENTRIES_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public EvictEntriesResponse<K> deserializeEvictEntriesResponse(Models.Message message) {
        return new EvictEntriesResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public Models.Message.Builder serializeUpdateEntriesNotification(UpdateEntriesNotification<K, V> notification) {
        var result = Models.Message.newBuilder().setType(MessageType.UPDATE_ENTRIES_NOTIFICATION.name());
        Utils.relayUuidToStringIfNotNull(notification::sourceEndpointId, result::setSourceId);
        var serializedEntries = this.serializeEntries(notification.entries());
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(notification::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public UpdateEntriesNotification<K, V> deserializeUpdateEntriesNotification(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new UpdateEntriesNotification<>(
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public Models.Message.Builder serializeUpdateEntriesRequest(UpdateEntriesRequest<K, V> request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.UPDATE_ENTRIES_REQUEST.name())
                ;

        var serializedEntries = this.serializeEntries(request.entries());

        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public UpdateEntriesRequest<K, V> deserializeUpdateEntriesRequest(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new UpdateEntriesRequest<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeUpdateEntriesResponse(UpdateEntriesResponse<K, V> response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.UPDATE_ENTRIES_RESPONSE.name())
                ;
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        var serializedEntries = this.serializeEntries(response.entries());
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public UpdateEntriesResponse<K, V> deserializeUpdateEntriesResponse(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new UpdateEntriesResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }


    public Models.Message.Builder serializeInsertEntriesRequest(InsertEntriesRequest<K, V> request) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.INSERT_ENTRIES_REQUEST.name())
                ;

        var serializedEntries = this.serializeEntries(request.entries());

        Utils.relayUuidToStringIfNotNull(request::requestId, result::setRequestId);
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(request::sourceEndpointId, result::setSourceId);
        return result;
    }

    public InsertEntriesRequest<K, V> deserializeInsertEntriesRequest(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new InsertEntriesRequest<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public Models.Message.Builder serializeInsertEntriesResponse(InsertEntriesResponse<K, V> response) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.INSERT_ENTRIES_RESPONSE.name())
                ;
        var serializedEntries = this.serializeEntries(response.existingEntries());
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(response::requestId, result::setRequestId);
        Utils.relayUuidToStringIfNotNull(response::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public Models.Message.Builder serializeInsertEntriesNotification(InsertEntriesNotification<K, V> notification) {
        var serializedEntries = this.serializeEntries(notification.entries());
        var result = Models.Message.newBuilder()
                .setType(MessageType.INSERT_ENTRIES_NOTIFICATION.name())
                ;
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(notification::sourceEndpointId, result::setSourceId);
        Utils.relayUuidToStringIfNotNull(notification::destinationEndpointId, result::setDestinationId);
        return result;
    }

    public InsertEntriesNotification<K, V> deserializeInsertEntriesNotification(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new InsertEntriesNotification<K, V>(
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId),
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public InsertEntriesResponse<K, V> deserializeInsertEntriesResponse(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new InsertEntriesResponse<>(
                Utils.supplyStringToUuidIfTrue(message.hasRequestId(), message::getRequestId),
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasDestinationId(), message::getDestinationId)
        );
    }

    public Models.Message.Builder serializeRemoveEntriesNotification(RemoveEntriesNotification<K, V> notification) {
        var result = Models.Message.newBuilder()
                .setType(MessageType.REMOVE_ENTRIES_NOTIFICATION.name())
                ;
        var serializedEntries = this.serializeEntries(notification.entries());
        Utils.relayIfNotNull(serializedEntries::getKey, result::addAllKeys);
        Utils.relayIfNotNull(serializedEntries::getValue, result::addAllValues);
        Utils.relayUuidToStringIfNotNull(notification::sourceEndpointId, result::setSourceId);
        return result;
    }

    public RemoveEntriesNotification<K, V> deserializeRemoveEntriesNotification(Models.Message message) {
        var entries = this.deserializeEntries(
                Utils.supplyIfTrue(0 < message.getKeysCount(), message::getKeysList),
                Utils.supplyIfTrue(0 < message.getValuesCount(), message::getValuesList)
        );
        return new RemoveEntriesNotification<>(
                entries,
                Utils.supplyStringToUuidIfTrue(message.hasSourceId(), message::getSourceId)
        );
    }

    public KeyValuePair<List<ByteString>, List<ByteString>> serializeEntries(Map<K, V> entries) {
        if (entries == null || entries.size() < 1) {
            return KeyValuePair.of(Collections.EMPTY_LIST, Collections.EMPTY_LIST);
        }
        var keys = new LinkedList<ByteString>();
        var values = new LinkedList<ByteString>();
        entries.forEach((entryKey, entryValue) -> {
            var key = this.keyEncoder.apply(entryKey);
            var value = this.valueEncoder.apply(entryValue);
            if (entryKey == null || entryValue == null) {
                return;
            }
            keys.add(UnsafeByteOperations.unsafeWrap(key));
            values.add(UnsafeByteOperations.unsafeWrap(value));
        });
        return KeyValuePair.of(keys, values);
    }

    public Map<K, V> deserializeEntries(List<ByteString> keys, List<ByteString> values) {
        if (keys == null || values == null || keys.size() < 1 || values.size() < 1) {
            return Collections.emptyMap();
        }
        var result = new HashMap<K, V>();
        var keyIt = keys.iterator();
        var valuesIt = values.iterator();
        while (keyIt.hasNext() && valuesIt.hasNext()) {
            var foundKey = keyIt.next();
            var foundValue = valuesIt.next();
            try {
//                var readonlyFoundKey = foundKey.asReadOnlyByteBuffer();
//                var readonlyFoundValue = foundValue.asReadOnlyByteBuffer();
//                foundKey.toByteArray()
//                com.google.protobuf.UnsafeByteOperations.
                var entryKey = this.keyDecoder.apply(foundKey.toByteArray());
                var entryValue = this.valueDecoder.apply(foundValue.toByteArray());
                if (entryKey != null && entryValue != null) {
                    result.put(entryKey, entryValue);
                }
            } catch (Exception ex) {
                logger.warn("Exception occurred while deserializing entries", ex);
                continue;
            }
        }
        return result;
    }

    public List<ByteString> serializeKeys(Collection<K> keys) {
        if (keys == null || keys.size() < 1) {
            return Collections.EMPTY_LIST;
        }

        return keys.stream()
                .map(this.keyEncoder)
                .filter(Objects::nonNull)
                .map(com.google.protobuf.UnsafeByteOperations::unsafeWrap)
                .collect(Collectors.toList());
    }

    public Set<K> deserializeKeys(List<ByteString> keys) {
        if (keys == null || keys.size() < 1) {
            return Collections.EMPTY_SET;
        }
        return keys.stream()
                .map(byteString -> this.keyDecoder.apply(byteString.toByteArray()))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }


}
