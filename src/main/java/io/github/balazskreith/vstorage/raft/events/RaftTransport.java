package io.github.balazskreith.vstorage.raft.events;

public interface RaftTransport {
    EventSender sender();
    EventReceiver receiver();

    static RaftTransport createFrom(Events inboundEvents, Events outboundEvents) {
        var receiver = EventReceiver.createFrom(inboundEvents);
        var sender = EventSender.createFrom(outboundEvents);

        return new RaftTransport() {
            @Override
            public EventSender sender() {
                return sender;
            }

            @Override
            public EventReceiver receiver() {
                return receiver;
            }
        };
    }
}
