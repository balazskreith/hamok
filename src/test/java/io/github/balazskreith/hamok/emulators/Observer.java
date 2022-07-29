package io.github.balazskreith.hamok.emulators;

import io.github.balazskreith.hamok.storagegrid.StorageGrid;

public class Observer {
    private final StorageGrid grid;
//    private final MultiStorageSet<UUID, UUID> callsToClients;
//    private final SeparatedStorage<UUID, OutboundTrack> outboundTracks;
//    private final SeparatedStorage<UUID, Client> clients;
//    private final ReplicatedStorage<String, Room> rooms;

    public Observer(String context) {
        this.grid = StorageGrid.builder()
                .withRaftMaxLogRetentionTimeInMs(30000)
                .withContext(context)
                .withAutoDiscovery(true)
                .build();

//        this.rooms = this.grid.<String, Room>replicatedStorage()
//                .setStorageId("rooms")
//                .setKeyCodecSupplier(() -> Codec.create(str -> str, str -> str))
//                .setValueCodecSupplier(() -> Codec.create(UUID::toString, UUID::fromString))
//                .build();
//
//        this.outboundTracks = this.grid.<UUID, OutboundTrack>separatedStorage()
//                .setStorageId("sentBytes")
//                .setKeyCodecSupplier(() -> Codec.create(UUID::toString, UUID::fromString))
//                .setValueCodecSupplier(() -> Codec.create(d -> d.toString(), Double::parseDouble))
//                .build();
//
//        var mapper = new ObjectMapper();
//        var callsToClients = this.grid.<UUID, Set<UUID>>replicatedStorage()
//                .setStorageId("callsToClients")
//                .setKeyCodecSupplier(() -> Codec.create(UUID::toString, UUID::fromString))
//                .setValueCodecSupplier(() -> Codec.create(Mapper.create(set -> mapper.writeValueAsString(set)), Mapper.create(str -> mapper.readValue(str, Set.class))))
//                .build();
//        this.callsToClients = MultiStorageSet.fromStorage(callsToClients);
    }

    public void accept(Sample sample) {
//        UUID callId = this.rooms.get(sample.roomId());
//        if (callId == null) {
//            callId = this.rooms.insert(sample.roomId(), UUID.randomUUID());
//        }
//
//        this.sentBytes.set(sample.clientId(), sample.sentBytes());
    }
}
