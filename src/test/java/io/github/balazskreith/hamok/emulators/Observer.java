package io.github.balazskreith.hamok.emulators;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.balazskreith.hamok.MultiStorageSet;
import io.github.balazskreith.hamok.mappings.Codec;
import io.github.balazskreith.hamok.mappings.Mapper;
import io.github.balazskreith.hamok.storagegrid.FederatedStorage;
import io.github.balazskreith.hamok.storagegrid.ReplicatedStorage;
import io.github.balazskreith.hamok.storagegrid.SeparatedStorage;
import io.github.balazskreith.hamok.storagegrid.StorageGrid;

import java.util.Set;
import java.util.UUID;

public class Observer {
    private final StorageGrid grid;
    final MultiStorageSet<UUID, UUID> callsToClients;
    final FederatedStorage<UUID, Double> callsSentBytes;
    final SeparatedStorage<UUID, Client> clients;
    final ReplicatedStorage<String, Call> calls;

    public Observer(StorageGrid grid) {
        this.grid = grid;

        this.calls = this.grid.<String, Call>replicatedStorage()
                .setStorageId("rooms")
                .setKeyCodecSupplier(() -> Codec.create(str -> str, str -> str))
                .setValueCodecSupplier(() -> Codec.create(Call::toString, Call::fromString))
                .build();

        this.clients = this.grid.<UUID, Client>separatedStorage()
                .setStorageId("clients")
                .setKeyCodecSupplier(() -> Codec.create(UUID::toString, UUID::fromString))
                .setValueCodecSupplier(() -> Codec.create(Client::toString, Client::fromString))
                .build();

        this.callsSentBytes = this.grid.<UUID, Double>federatedStorage()
                .setStorageId("callsSentBytes")
                .setKeyCodecSupplier(() -> Codec.create(UUID::toString, UUID::fromString))
                .setValueCodecSupplier(() -> Codec.create(d -> d.toString(), Double::parseDouble))
                .setMergeOperator(() -> (sentBytes1, sentBytes2) -> sentBytes1 + sentBytes2)
                .build();

        var mapper = new ObjectMapper();
        var callsToClients = this.grid.<UUID, Set<UUID>>replicatedStorage()
                .setStorageId("callsToClients")
                .setKeyCodecSupplier(() -> Codec.create(UUID::toString, UUID::fromString))
                .setValueCodecSupplier(() -> Codec.create(Mapper.create(set -> mapper.writeValueAsString(set)), Mapper.create(str -> mapper.readValue(str, Set.class))))
                .build();
        this.callsToClients = MultiStorageSet.fromStorage(callsToClients);
    }

    public void accept(Sample sample) {
        var call = this.calls.get(sample.roomId());
        if (call == null) {
            call = new Call(
                    sample.roomId(),
                    UUID.randomUUID()
            );
            call = this.calls.insert(sample.roomId(), call);
        }

        var client = this.clients.get(sample.clientId());
        if (client == null) {
            client = new Client(sample.clientId());
            if (this.clients.set(client.clientId(), client) != null) {
                throw new RuntimeException("Clients must be set once");
            }
            this.callsToClients.add(call.callId(), client.clientId());
        }
        this.callsSentBytes.set(call.callId(), sample.sentBytes());
    }
}
