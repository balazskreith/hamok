package io.github.balazskreith.hamok.racoon.events;

public interface RacoonTransport {
    OutboundEvents sender();
    InboundEvents receiver();

    static RacoonTransport createFrom(Events inboundEvents, Events outboundEvents) {
        var receiver = InboundEvents.createFrom(inboundEvents);
        var sender = OutboundEvents.createFrom(outboundEvents);

        return new RacoonTransport() {
            @Override
            public OutboundEvents sender() {
                return sender;
            }

            @Override
            public InboundEvents receiver() {
                return receiver;
            }
        };
    }
}
