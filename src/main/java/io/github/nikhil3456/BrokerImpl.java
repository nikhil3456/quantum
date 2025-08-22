package io.github.nikhil3456;

import java.util.List;
import java.util.concurrent.*;

public class BrokerImpl implements Broker {
    private final ConcurrentHashMap<String, Topic> topics = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<ConsumerWorker>> consumerWorkers = new ConcurrentHashMap<>();

    private final ExecutorService producerExecutor;
    private final ExecutorService consumerExecutor;
    private volatile boolean isShutdown = false;

    public BrokerImpl() {
        this.producerExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.consumerExecutor = Executors.newCachedThreadPool();
    }

    @Override
    public void createTopic(String topicName) {
        topics.putIfAbsent(topicName, new Topic(topicName));
    }

    @Override
    public CompletableFuture<Receipt> enqueue(Message message) {
        if (isShutdown) {
            return CompletableFuture.failedFuture(new IllegalStateException("Broker is shutting down."));
        }

        return CompletableFuture.supplyAsync(() -> {
            Topic topic  = topics.get(message.topic());
            if (topic == null) {
                throw new IllegalArgumentException("Topic not found: " + message.topic());
            }
            topic.addMessage(message);
            return new Receipt(message.id());
        }, producerExecutor);
    }

    public void subscribe(String topicName, String groupId, IConsumer consumer) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            throw new IllegalArgumentException("Topic not found: " + topicName);
        }

        String consumerKey = topicName + "-" + groupId;
        ConsumerWorker worker = new ConsumerWorker(topicName, groupId, consumer, this, consumerExecutor);
        consumerWorkers.get(consumerKey).add(worker);

        new Thread(worker).start();
    }

    public Message poll(String topicName, String groupId) {
        Topic topic = topics.get(topicName);
        if (topic == null) return null;

        return topic.getMessageForConsumer(groupId);
    }

    public void commit(String topicName, String groupId, String messageId) {
        Topic topic = topics.get(topicName);
        if (topic != null) {
            topic.commitOffset(groupId, messageId);
        }
    }

    public void shutdown() {
        isShutdown = true;

        producerExecutor.shutdown();
        try {
            if (!producerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                producerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            producerExecutor.shutdownNow();
        }

        consumerWorkers.values().forEach(workers -> workers.forEach(ConsumerWorker::stop));

        consumerExecutor.shutdown();
        try {
            if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                consumerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            consumerExecutor.shutdownNow();
        }
    }

}
