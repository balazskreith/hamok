package io.github.balazskreith.hamok.storagegrid;

import io.github.balazskreith.hamok.storagegrid.messages.Message;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface GridActor {

    String getIdentifier();
    void accept(Message message);
    void close();

    boolean executeSync();

    static Builder builder() {
        return new Builder();
    }

    class Builder {
        private String identifier;
        private Consumer<Message> messageListener;
        private Runnable closeAction;
        private Supplier<Boolean> syncExecutor;

        public GridActor build() {
            Objects.requireNonNull(this.identifier);
            Objects.requireNonNull(this.messageListener);
            Objects.requireNonNull(this.closeAction);

            return new GridActor() {
                @Override
                public String getIdentifier() {
                    return identifier;
                }

                @Override
                public void accept(Message message) {
                    messageListener.accept(message);
                }

                @Override
                public void close() {
                    closeAction.run();
                }

                @Override
                public boolean executeSync() { return syncExecutor.get(); }
            };
        }

        public Builder setIdentifier(String value) {
            this.identifier = value;
            return this;
        }

        public Builder setMessageAcceptor(Consumer<Message> receiver) {
            this.messageListener = receiver;
            return this;
        }

        public Builder setCloseAction(Runnable action) {
            this.closeAction = action;
            return this;
        }

        public Builder setSyncExecutor(Supplier<Boolean> syncExecutor) {
            this.syncExecutor = syncExecutor;
            return this;
        }
    }
}
