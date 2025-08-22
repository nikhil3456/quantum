package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class ConsumerWorker implements Runnable {

    private final String topicName;
    private final String groupId;
    private final IConsumer consumerLogic;
    private final Broker broker;
    private final ExecutorService consumerExecutor;
    private volatile boolean isRunning = true;

    public ConsumerWorker(String topicName, String groupId, IConsumer consumerLogic, Broker broker, ExecutorService consumerExecutor) {
        this.topicName = topicName;
        this.groupId = groupId;
        this.consumerLogic = consumerLogic;
        this.broker = broker;
        this.consumerExecutor = consumerExecutor;
    }

    @Override
    public void run() {
        while (isRunning) {
            try {
                Message message = broker.poll(topicName, groupId);
                if (message != null) {
                    CompletableFuture<Void> processingFuture = consumerLogic.process(message);

                    processingFuture.whenCompleteAsync((result, exception) -> {
                        if (exception == null) {
                            broker.commit(topicName, groupId, message.id());
                        } else {
                            System.err.printf("ERROR: Consumer %s failed to process message %s. Error: %s. Message will be redelivered.%n",
                                    consumerLogic.getConsumerGroupId(), message.id(), exception.getMessage());
                        }
                    }, consumerExecutor);
                } else {
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                isRunning = false;
            }
        }
    }

    public void stop() {
        isRunning = false;
    }
}
