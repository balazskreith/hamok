package io.github.balazskreith.hamok.raccoons;

import io.github.balazskreith.hamok.common.RwLock;
import io.github.balazskreith.hamok.common.UuidTools;
import io.github.balazskreith.hamok.raccoons.events.RaftAppendEntriesRequestChunk;
import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class RaftAppendEntriesRequest {

    private static Logger logger = LoggerFactory.getLogger(RaftAppendEntriesRequest.class);

    private UUID peerId = null;
    private int term = -1;
    private UUID leaderId = null;
    private int prevLogIndex = -1;
    private int prevLogTerm = -1;
    private int leaderCommit = -1;
    private int leaderNextIndex = -1;
    private Map<Integer, Message> entries = new HashMap<>();

    private boolean ready = false;
    private RwLock rwLock = new RwLock();
    private int endSeq = -1;
    private final long createdInSec = Instant.now().getEpochSecond();
    private int received = 0;

    private final AtomicReference<List<Message>> entriesList = new AtomicReference<>(Collections.emptyList());
    private final UUID requestId;

    RaftAppendEntriesRequest(UUID requestId) {
        this.requestId = requestId;
    }

    public void add(RaftAppendEntriesRequestChunk requestChunk) {
//        logger.info("{}", requestChunk);
        if (UuidTools.notEquals(requestChunk.requestId(), this.requestId)) {
            logger.warn("RequestId for Raft Append Request is not equal to the provided AppendRequestChunk requestId: {} != {}", this.requestId, requestChunk.requestId());
            return;
        }
        this.rwLock.runInWriteLock(() -> this.process(requestChunk));
    }

    private void process(RaftAppendEntriesRequestChunk requestChunk) {
        if (this.peerId == null) {
            this.peerId = requestChunk.peerId();
        }
        if (this.term < 0) {
            this.term = requestChunk.term();
        }
        if (this.leaderId == null) {
            this.leaderId = requestChunk.leaderId();
        }
        if (this.prevLogIndex < 0) {
            this.prevLogIndex = requestChunk.prevLogIndex();
        }
        if (this.prevLogTerm < 0) {
            this.prevLogTerm = requestChunk.prevLogTerm();
        }
        if (this.leaderCommit < 0) {
            this.leaderCommit = requestChunk.leaderCommit();
        }
        if (this.leaderNextIndex < 0) {
            this.leaderNextIndex = requestChunk.leaderNextIndex();
        }
        if (requestChunk.entry() != null) {
            this.entries.put(requestChunk.sequence(), requestChunk.entry());
        }
        if (requestChunk.lastMessage()) {
            this.endSeq = requestChunk.sequence();
        }
        if (this.endSeq == 0) {
            // in this case the first message is the last,
            // so its immediately ready
            this.ready = true;
        } else if (0 < this.endSeq) {
            // in this case the saved number of entries have to be equal to the endseq
            this.ready = (this.entries.size() - 1) == this.endSeq;
        }
        if (this.ready) {
            var entriesList = this.entries.values().stream().collect(Collectors.toList());
            this.entriesList.set(entriesList);
        }
    }

    public UUID peerId() {
        return this.peerId;
    }

    public int term() {
        return this.term;
    }

    public UUID leaderId() {
        return this.leaderId;
    }

    public int prevLogIndex() {
        return this.prevLogIndex;
    }

    public int prevLogTerm() {
        return this.prevLogTerm;
    }

    public int leaderCommit() {
        return this.leaderCommit;
    }

    public int leaderNextIndex() {
        return this.leaderNextIndex;
    }

    public List<Message> entries() {
        return this.entriesList.get();
    }

    public boolean ready() {
        return this.ready;
    }
    @Override
    public String toString() {
        return String.format("{ " +
                "peerId: %s, " +
                "term: %d, " +
                "leaderId: %s, " +
                "prevLogIndex: %d, " +
                "prevLogTerm: %d, " +
                "leaderCommit: %d, " +
                "leaderNextIndex: %d," +
                "endSeq: %d, " +
                "ready: %s " +
                "}",
                this.peerId,
                this.term,
                this.leaderId,
                this.prevLogIndex,
                this.prevLogTerm,
                this.leaderCommit,
                this.leaderNextIndex,
                this.endSeq,
                this.ready
        );
    }
}
