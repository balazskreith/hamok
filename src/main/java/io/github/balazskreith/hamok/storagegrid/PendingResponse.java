package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class PendingResponse {
    private static final Logger logger = LoggerFactory.getLogger(PendingResponse.class);

    private final Map<Integer, Models.Message> messages = new ConcurrentHashMap<>();
    private volatile int endSeq = -1;
    private AtomicReference<Models.Message> result = new AtomicReference<>(null);

    public PendingResponse() {

    }

    public void accept(Models.Message message) {
        if (this.result.get() != null) {
            logger.warn("Pending Response is already assembled, newly received message is not accepted {}", message);
            return;
        }
        var sequence = Utils.supplyIfTrue(message.hasSequence(), message::getSequence);
        var lastMessage = Utils.supplyIfTrue(message.hasLastMessage(), message::getLastMessage);
        var removedMessage = this.messages.put(sequence, message);
        if (removedMessage != null) {
            logger.warn("Duplicated sequence detected for pending response by receiving removedMessage: {}, actual messages {}", removedMessage, message);
        }
        if (Boolean.TRUE.equals(lastMessage)) {
            this.endSeq = sequence;
        }
        if (this.endSeq < 0) {
            return;
        }
        if (this.endSeq == 0) {
            this.result.set(this.messages.remove(this.endSeq));
            return;
        }

        if (this.messages.size() != this.endSeq + 1) {
            return;
        }
        var response = Models.Message.newBuilder(this.messages.get(0));
        if (0 < response.getKeysCount()) {
            response.clearKeys();
        }
        if (0 < response.getValuesCount()) {
            response.clearValues();
        }
        if (response.hasSequence()) {
            response.clearSequence();
        }
        if (response.hasLastMessage()) {
            response.clearLastMessage();
        }
        for (int seq = 0; seq <= this.endSeq; ++seq) {
            var responseChunk = this.messages.get(seq);
            if (0 < responseChunk.getKeysCount()) {
                response.addAllKeys(responseChunk.getKeysList());
            }
            if (0 < responseChunk.getValuesCount()) {
                response.addAllValues(responseChunk.getValuesList());
            }
        }
        this.result.set(response.build());
    }

    public boolean isReady() {
        return this.result.get() != null;
    }

    public Models.Message getResult() {
        return this.result.get();
    }
}
