package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

class ResponseMessageChunker implements Function<Message, Iterator<Message>> {
    private static final Logger logger = LoggerFactory.getLogger(ResponseMessageChunker.class);

    private static Iterator<Message> EMPTY_ITERATOR = new Iterator<Message>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Message next() {
            return null;
        }
    };

    public static Function<Message, Iterator<Message>> createSelfIteratorProvider() {
        return message -> createSelfIterator(message);
    }

    private final int maxKeys;
    private final int maxValues;
    private final int maxEntries;

    public ResponseMessageChunker(int maxKeys, int maxValues) {
        this.maxKeys = maxKeys;
        this.maxValues = maxValues;
        if (this.maxKeys < 1 && this.maxValues < 1) {
            this.maxEntries = 0;
        } else if (this.maxKeys < 1) {
            this.maxEntries = this.maxValues;
        } else if (this.maxValues < 1) {
            this.maxEntries = this.maxKeys;
        } else {
            this.maxEntries = Math.min(this.maxKeys, this.maxValues);
        }
    }

    @Override
    public Iterator<Message> apply(Message message) {
        if (message == null) {
            return EMPTY_ITERATOR;
        }
        if (message.keys == null && message.values == null) {
            return createSelfIterator(message);
        } else if (message.keys != null && message.values != null) {
            return this.createEntriesIterator(message);
        } else if (message.keys != null) {
            return this.createKeysIterator(message);
        } else {
            logger.warn("Cannot make an iterator for message, because it does not have keys and values, or just keys", message);
            return createSelfIterator(message);
        }
    }

    private Iterator<Message> createKeysIterator(Message message) {
        if (message.keys.size() <= this.maxKeys) {
            return createSelfIterator(message);
        }
        var keysIt = message.keys.iterator();
        return new Iterator<Message>() {
            private int sequence = -1;
            @Override
            public boolean hasNext() {
                return keysIt.hasNext();
            }

            @Override
            public Message next() {
                var result = message.makeCopy();
                result.keys = new LinkedList<>();
                for (int i = 0; i < maxKeys && keysIt.hasNext(); ++i) {
                    var key = keysIt.next();
                    result.keys.add(key);
                }
                result.sequence = ++sequence;
                result.lastMessage = keysIt.hasNext() == false;
                return result;
            }
        };
    }

    private Iterator<Message> createEntriesIterator(Message message) {
        if (Math.max(message.keys.size(), message.values.size()) <= this.maxEntries) {
            return createSelfIterator(message);
        }
        var keysIt = message.keys.iterator();
        var valuesIt = message.values.iterator();
        return new Iterator<Message>() {
            private int sequence = -1;
            @Override
            public boolean hasNext() {
                return keysIt.hasNext() && valuesIt.hasNext();
            }

            @Override
            public Message next() {
                var result = message.makeCopy();
                result.keys = new LinkedList<>();
                result.values = new LinkedList<>();
                for (int i = 0; i < maxEntries && keysIt.hasNext() && valuesIt.hasNext(); ++i) {
                    var key = keysIt.next();
                    var value = valuesIt.next();
                    result.keys.add(key);
                    result.values.add(value);
                }
                result.sequence = ++sequence;
                result.lastMessage = !keysIt.hasNext() || !valuesIt.hasNext();
                return result;
            }
        };
    }


    private static Iterator<Message> createSelfIterator(Message message) {
        var value = new AtomicReference<Message>(message);
        return new Iterator<Message>() {
            @Override
            public boolean hasNext() {
                return value.get() != null;
            }

            @Override
            public Message next() {
                return value.getAndSet(null);
            }
        };
    }
}
