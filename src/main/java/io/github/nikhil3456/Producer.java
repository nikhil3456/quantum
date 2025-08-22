package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;

public class Producer implements IProducer {
    private final Broker broker;

    public Producer(Broker broker) {
        this.broker = broker;
    }

    @Override
    public CompletableFuture<Receipt> send(Message message) {
        return broker.enqueue(message);
    }
}
