package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;

public interface Broker {
    void createTopic(String topicName);
    CompletableFuture<Receipt> enqueue(Message message);
    Message poll(String topicName, String consumerGroupId);

    void commit(String topicName, String consumerGroupId, String messageId);
}
