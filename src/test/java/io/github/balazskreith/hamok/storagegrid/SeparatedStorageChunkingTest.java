//package io.github.balazskreith.hamok.storagegrid;
//
//import io.github.balazskreith.hamok.storagegrid.messages.Message;
//import io.github.balazskreith.hamok.storagegrid.messages.MessageType;
//import org.junit.jupiter.api.*;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.nio.ByteBuffer;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.function.Function;
//
//@DisplayName("Replicated Storage Insert operation scenario.")
//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
//class SeparatedStorageChunkingTest {
//
//    private static final Logger logger = LoggerFactory.getLogger(SeparatedStorageChunkingTest.class);
//
//    private static final int EXPIRATION_TIME_IN_MS = 15000;
//    private static final String STORAGE_ID = "Stockpiles";
//
//    private StorageGrid euWest;
//    private StorageGrid usEast;
//    private SeparatedStorage<Integer, Integer> bcnStockpile;
//    private SeparatedStorage<Integer, Integer> nyStockpile;
//    private Map<String, List<Message>> nyOutboundMessages = new ConcurrentHashMap<>();
//    private Map<String, List<Message>> bcnOutboundMessages = new ConcurrentHashMap<>();
//
//    @Test
//    @Order(1)
//    @DisplayName("Setup")
//    void test_1() throws InterruptedException, ExecutionException, TimeoutException {
//        this.euWest = StorageGrid.builder()
//                .withContext("Eu West")
//                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
//                .build();
//        this.usEast = StorageGrid.builder()
//                .withContext("US east")
//                .withRaftMaxLogRetentionTimeInMs(EXPIRATION_TIME_IN_MS)
//                .build();
//        Function<Integer, byte[]> intEnc = i -> ByteBuffer.allocate(4).putInt(i).array();
//        Function<byte[], Integer> intDec = b -> ByteBuffer.wrap(b).getInt();
//
//        bcnStockpile = euWest.<Integer, Integer>separatedStorage()
//                .setStorageId(STORAGE_ID)
//                .setKeyCodec(intEnc, intDec)
//                .setValueCodec(intEnc, intDec)
//                .setMaxMessageKeys(2)
//                .setMaxMessageValues(2)
//                .build();
//
//        nyStockpile = usEast.<Integer, Integer>separatedStorage()
//                .setStorageId(STORAGE_ID)
//                .setKeyCodec(intEnc, intDec)
//                .setValueCodec(intEnc, intDec)
//                .setMaxMessageKeys(2)
//                .setMaxMessageValues(2)
//                .build();
//
//        var euWestIsReady = new CompletableFuture<UUID>();
//        var usEastIsReady = new CompletableFuture<UUID>();
//
//        euWest.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(euWestIsReady::complete);
//        usEast.changedLeaderId().filter(Optional::isPresent).map(Optional::get).subscribe(usEastIsReady::complete);
//
//        euWest.transport().getSender().map(message -> {
//            var messages = this.bcnOutboundMessages.get(message.type);
//            if (messages == null) {
//                messages = new LinkedList<>();
//                this.bcnOutboundMessages.put(message.type, messages);
//            }
//            messages.add(message);
//            return message;
//        }).subscribe(usEast.transport().getReceiver());
//
//        usEast.transport().getSender().map(message -> {
//            var messages = this.nyOutboundMessages.get(message.type);
//            if (messages == null) {
//                messages = new LinkedList<>();
//                this.nyOutboundMessages.put(message.type, messages);
//            }
//            messages.add(message);
//            return message;
//        }).subscribe(euWest.transport().getReceiver());
//
//        CompletableFuture.allOf(euWestIsReady, usEastIsReady).get(15000, TimeUnit.MILLISECONDS);
//        Assertions.assertEquals(euWestIsReady.get(), usEastIsReady.get());
//    }
//
//    @Test
//    @Order(2)
//    @DisplayName("Should chunk request")
//    void test_2() throws ExecutionException, InterruptedException, TimeoutException {
//        bcnStockpile.setAll(Map.of(
//                1, 1,
//                2, 2,
//                3,3,
//                4,4
//        ));
//        Assertions.assertEquals(2, this.bcnOutboundMessages.get(MessageType.UPDATE_ENTRIES_REQUEST.name()).size());
//        Assertions.assertEquals(2, this.nyOutboundMessages.get(MessageType.UPDATE_ENTRIES_RESPONSE.name()).size());
//        this.bcnOutboundMessages.clear();
//        this.nyOutboundMessages.clear();
//    }
//
//    @Test
//    @Order(3)
//    @DisplayName("Should chunk response")
//    void test_3() throws ExecutionException, InterruptedException, TimeoutException {
//        nyStockpile.keys();
//        Assertions.assertEquals(1, this.nyOutboundMessages.get(MessageType.GET_KEYS_REQUEST.name()).size());
//        Assertions.assertEquals(2, this.bcnOutboundMessages.get(MessageType.GET_KEYS_RESPONSE.name()).size());
//        this.bcnOutboundMessages.clear();
//        this.nyOutboundMessages.clear();
//    }
//
//}