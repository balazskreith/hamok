package io.github.balazskreith.hamok.storagegrid.messages;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

class StorageOpSerDeTest {

    static StorageOpSerDe<Integer, String> storageOpSerDe;

    @BeforeAll
    static void setup() {
        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
        storageOpSerDe = new StorageOpSerDe<Integer, String>(
                intEnc,
                intDec,
                String::getBytes,
                String::new
        );
    }

    @Test
    void clearEntriesNotification() {
        var expected = new ClearEntriesNotification(
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeClearEntriesNotification(expected);
        var actual = storageOpSerDe.deserializeClearEntriesNotification(message.build());

        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }


    @Test
    void clearEntriesRequest() {
        var expected = new ClearEntriesRequest(
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeClearEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeClearEntriesRequest(message.build());

        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }


    @Test
    void clearEntriesResponse() {
        var expected = new ClearEntriesResponse(
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeClearEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeClearEntriesResponse(message.build());

        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());

    }

    @Test
    void getKeysRequest() {
        var expected = new GetKeysRequest(
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeGetKeysRequest(expected);
        var actual = storageOpSerDe.deserializeGetEntriesRequest(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }

    @Test
    void getKeysResponse() {
        var expected = new GetKeysResponse<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeGetKeysResponse(expected);
        var actual = storageOpSerDe.deserializeGetKeysResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
    }


    @Test
    void getSizeRequest() {
        var expected = new GetSizeRequest(
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeGetSizeRequest(expected);
        var actual = storageOpSerDe.deserializeGetSizeRequest(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }


    @Test
    void getSizeResponse() {
        var expected = new GetSizeResponse<>(
                UUID.randomUUID(),
                1,
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeGetSizeResponse(expected);
        var actual = storageOpSerDe.deserializeGetSizeResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
    }

    @Test
    void getEntriesRequest() {
        var expected = new GetEntriesRequest<>(
                Set.of(1),
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeGetEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeGetEntriesRequest(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }

    @Test
    void getEntriesResponse() {
        var expected = new GetEntriesResponse<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeGetEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeGetEntriesResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.foundEntries().size(), actual.foundEntries().size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
    }

    @Test
    void deleteEntriesNotification() {
        var expected = new DeleteEntriesNotification<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeDeleteEntriesNotification(expected);
        var actual = storageOpSerDe.deserializeDeleteEntriesNotification(message.build());

        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
    }


    @Test
    void deleteEntriesRequest() {
        var expected = new DeleteEntriesRequest<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeDeleteEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeDeleteEntriesRequest(message.build());

        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void deleteEntriesResponse() {
        var expected = new DeleteEntriesResponse<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeDeleteEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeDeleteEntriesResponse(message.build());

        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.deletedKeys().size(), actual.deletedKeys().size());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }


    @Test
    void removeEntriesRequest() {
        var expected = new RemoveEntriesRequest<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeRemoveEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeRemoveEntriesRequest(message.build());

        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void removeEntriesResponse() {
        var expected = new RemoveEntriesResponse<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeRemoveEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeRemoveEntriesResponse(message.build());

        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.removedEntries().size(), actual.removedEntries().size());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void evictEntriesNotification() {
        var expected = new EvictEntriesNotification<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeEvictEntriesNotification(expected);
        var actual = storageOpSerDe.deserializeEvictEntriesNotification(message.build());

        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }

    @Test
    void evictEntriesRequest() {
        var expected = new EvictEntriesRequest<>(
                UUID.randomUUID(),
                Set.of(1),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeEvictEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeEvictEntriesRequest(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.keys().size(), actual.keys().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }

    @Test
    void evictEntriesResponse() {
        var expected = new EvictEntriesResponse<Integer>(
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeEvictEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeEvictEntriesResponse(message.build());

        Assertions.assertEquals(expected.requestId(), actual.requestId());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
    }

    @Test
    void updateEntriesNotification() {
        var expected = new UpdateEntriesNotification<>(
                Map.of(1, "one"),
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeUpdateEntriesNotification(expected);
        var actual = storageOpSerDe.deserializeUpdateEntriesNotification(message.build());

        Assertions.assertEquals(expected.entries().size(), actual.entries().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
    }

    @Test
    void updateEntriesRequest() {
        var expected = new UpdateEntriesRequest<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeUpdateEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeUpdateEntriesRequest(message.build());

        Assertions.assertEquals(expected.entries().size(), actual.entries().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void updateEntriesResponse() {
        var expected = new UpdateEntriesResponse<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeUpdateEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeUpdateEntriesResponse(message.build());

        Assertions.assertEquals(expected.entries().size(), actual.entries().size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }


    @Test
    void serializeInsertEntriesRequest() {
        var expected = new InsertEntriesRequest<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeInsertEntriesRequest(expected);
        var actual = storageOpSerDe.deserializeInsertEntriesRequest(message.build());

        Assertions.assertEquals(expected.entries().size(), actual.entries().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void InsertEntriesResponse() {
        var expected = new InsertEntriesResponse<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeInsertEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeInsertEntriesResponse(message.build());

        Assertions.assertEquals(expected.existingEntries().size(), actual.existingEntries().size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void serializeInsertEntriesNotification() {
        var expected = new InsertEntriesNotification<>(
                Map.of(1, "one"),
                UUID.randomUUID(),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeInsertEntriesNotification(expected);
        var actual = storageOpSerDe.deserializeInsertEntriesNotification(message.build());

        Assertions.assertEquals(expected.entries().size(), actual.entries().size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }

    @Test
    void deserializeInsertEntriesResponse() {
        var expected = new InsertEntriesResponse<>(
                UUID.randomUUID(),
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeInsertEntriesResponse(expected);
        var actual = storageOpSerDe.deserializeInsertEntriesResponse(message.build());

        Assertions.assertEquals(expected.existingEntries().size(), actual.existingEntries().size());
        Assertions.assertEquals(expected.destinationEndpointId(), actual.destinationEndpointId());
        Assertions.assertEquals(expected.requestId(), actual.requestId());
    }

    @Test
    void serializeRemoveEntriesNotification() {
        var expected = new RemoveEntriesNotification<>(
                Map.of(1, "one"),
                UUID.randomUUID()
        );
        var message = storageOpSerDe.serializeRemoveEntriesNotification(expected);
        var actual = storageOpSerDe.deserializeInsertEntriesNotification(message.build());

        Assertions.assertEquals(expected.entries().size(), actual.entries().size());
        Assertions.assertEquals(expected.sourceEndpointId(), actual.sourceEndpointId());
    }

    @Test
    void serializeEntries() {
        var expected = Map.of(1, "one", 2, "two");
        var serializeEntries = storageOpSerDe.serializeEntries(expected);
        var actual = storageOpSerDe.deserializeEntries(serializeEntries.getKey(), serializeEntries.getValue());

        Assertions.assertEquals(expected.get(1), actual.get(1));
        Assertions.assertEquals(expected.get(2), actual.get(2));
    }

    @Test
    void serializeKeys() {
        var expected = Set.of(1, 2);
        var serializedKeys = storageOpSerDe.serializeKeys(expected);
        var actual = storageOpSerDe.deserializeKeys(serializedKeys);

        Assertions.assertEquals(expected.size(), actual.size());
        Assertions.assertTrue(expected.contains(1));
        Assertions.assertTrue(expected.contains(2));
    }
}