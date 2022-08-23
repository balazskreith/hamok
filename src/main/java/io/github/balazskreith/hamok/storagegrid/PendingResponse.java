package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

class PendingResponse {
    private static final Logger logger = LoggerFactory.getLogger(PendingResponse.class);

    private final Map<Integer, Message> messages = new ConcurrentHashMap<>();
    private volatile int endSeq = -1;
    private AtomicReference<Message> result = new AtomicReference<>(null);

    public PendingResponse() {

    }

    public void accept(Message message) {
        if (this.result.get() != null) {
            logger.warn("Pending Response is already assembled, newly received message is not accepted {}", message);
            return;
        }
        var removedMessage = this.messages.put(message.sequence, message);
        if (removedMessage != null) {
            logger.warn("Duplicated sequence detected for pending response by receiving message {}", message);
        }
        if (Boolean.TRUE.equals(message.lastMessage)) {
            this.endSeq = message.sequence;
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
        var response = this.messages.get(0).makeCopy();
        response.keys = new LinkedList<>();
        if (response.values != null) {
            response.values = new LinkedList<>();
        }
        response.sequence = null;
        response.lastMessage = null;
        for (int seq = 0; seq <= this.endSeq; ++seq) {
            var responseChunk = this.messages.get(seq);
            response.keys.addAll(responseChunk.keys);
            if (responseChunk.values != null) {
                response.values.addAll(responseChunk.values);
            }
        }
        this.result.set(response);
    }

    public boolean isReady() {
        return this.result.get() != null;
    }

    public Message getResult() {
        return this.result.get();
    }
}
