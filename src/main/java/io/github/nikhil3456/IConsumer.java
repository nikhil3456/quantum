package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;

public interface IConsumer {
    void subscribe(String topic, String consumerGroupId);
    CompletableFuture<Void> process(Message message);
    String getConsumerGroupId();
}
