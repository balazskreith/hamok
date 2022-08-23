package io.github.balazskreith.hamok.storagegrid;

import com.google.protobuf.ByteString;
import io.github.balazskreith.hamok.Models;
import io.github.balazskreith.hamok.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

class ResponseMessageChunker implements Function<Models.Message.Builder, Iterator<Models.Message.Builder>> {
    private static final Logger logger = LoggerFactory.getLogger(ResponseMessageChunker.class);

    private static Iterator<Models.Message.Builder> EMPTY_ITERATOR = new Iterator<Models.Message.Builder>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Models.Message.Builder next() {
            return null;
        }
    };

    public static Function<Models.Message.Builder, Iterator<Models.Message.Builder>> createSelfIteratorProvider() {
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
    public Iterator<Models.Message.Builder> apply(Models.Message.Builder message) {
        if (message == null) {
            return EMPTY_ITERATOR;
        }
        if (message.getKeysCount() < 1 && message.getValuesCount() < 1) {
            return createSelfIterator(message);
        } else if (0 < message.getKeysCount() && 0 < message.getValuesCount()) {
            return createEntriesIterator(message);
        } else if (0 < message.getKeysCount()) {
            return createKeysIterator(message);
        } else {
            logger.warn("Cannot make an iterator for message, because it does not have keys and values, or just keys", message);
            return createSelfIterator(message);
        }
    }

    private Iterator<Models.Message.Builder> createKeysIterator(Models.Message.Builder message) {
        if (message.getKeysCount() <= this.maxKeys) {
            return createSelfIterator(message);
        }
        var keysIt = message.getKeysList().iterator();
        return new Iterator<Models.Message.Builder>() {
            private volatile int sequence = 0;
            @Override
            public boolean hasNext() {
                return keysIt.hasNext();
            }

            @Override
            public Models.Message.Builder next() {
//                var result = message.makeCopy();
                var result = Models.Message.newBuilder(message.build());
                if (0 < result.getKeysCount()) {
                    result.clearKeys();
                }
                for (int i = 0; i < maxKeys && keysIt.hasNext(); ++i) {
                    var key = keysIt.next();
                    result.addKeys(key);
                }
                result.setSequence(sequence);
                result.setLastMessage(keysIt.hasNext() == false);
                ++this.sequence;
                return result;
            }
        };
    }

    private Iterator<Models.Message.Builder> createEntriesIterator(Models.Message.Builder message) {
        if (Math.max(message.getKeysCount(), message.getValuesCount()) < this.maxEntries) {
            return createSelfIterator(message);
        }
        var keysIt = Utils.supplyIfTrueOrElse(0 < message.getKeysCount(), message.getKeysList()::iterator, Collections.<ByteString>emptyList().iterator());
        var valuesIt = Utils.supplyIfTrueOrElse(0 < message.getValuesCount(), message.getValuesList()::iterator, Collections.<ByteString>emptyList().iterator());
        return new Iterator<Models.Message.Builder>() {
            private volatile int sequence = 0;
            @Override
            public boolean hasNext() {
                return keysIt.hasNext() && valuesIt.hasNext();
            }

            @Override
            public Models.Message.Builder next() {
                var result = Models.Message.newBuilder(message.build());
                if (0 < result.getKeysCount()) {
                    result.clearKeys();
                }
                if (0 < result.getValuesCount()) {
                    result.clearValues();
                }
                for (int i = 0; i < maxEntries && keysIt.hasNext() && valuesIt.hasNext(); ++i) {
                    var key = keysIt.next();
                    var value = valuesIt.next();
                    result.addKeys(key);
                    result.addValues(value);
                }
                result.setSequence(sequence);
                result.setLastMessage(keysIt.hasNext() == false || valuesIt.hasNext() == false);
                ++this.sequence;
                return result;
            }
        };
    }


    private static Iterator<Models.Message.Builder> createSelfIterator(Models.Message.Builder message) {
        var value = new AtomicReference<Models.Message.Builder>(message);
        return new Iterator<Models.Message.Builder>() {
            @Override
            public boolean hasNext() {
                return value.get() != null;
            }

            @Override
            public Models.Message.Builder next() {
                return value.getAndSet(null);
            }
        };
    }
}
