package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

class EngineeringConsumer implements IConsumer {
    private final String id;
    public EngineeringConsumer(String id) {
        this.id = id;
    }

    @Override
    public CompletableFuture<Void> process(Message message) {
        return CompletableFuture.runAsync(() -> {
            try {
                System.out.printf("Engineering consumer '%s' START processing: %s%n", id, message.payload());
                // Simulate I/O or heavy computation
                Thread.sleep(ThreadLocalRandom.current().nextInt(500, 1000));
                System.out.printf("Engineering consumer '%s' END processing: %s%n", id, message.payload());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Override
    public String getConsumerId() {
        return this.id;
    }
}

// Example consumer implementation for the finance team
class FinanceConsumer implements IConsumer {
    private final String id;
    public FinanceConsumer(String id) { this.id = id; }

    @Override
    public String getConsumerId() { return id; }

    @Override
    public CompletableFuture<Void> process(Message message) {
        return CompletableFuture.runAsync(() -> {
            try {
                System.out.printf("Finance consumer '%s' START processing: %s%n", id, message.payload());
                // Simulate a faster process
                Thread.sleep(ThreadLocalRandom.current().nextInt(100, 300));
                if (message.payload().contains("5")) { // Simulate a processing failure
                    System.err.printf("Finance consumer '%s' FAILED processing: %s%n", id, message.payload());
                    throw new RuntimeException("Payment processing failed!");
                }
                System.out.printf("Finance consumer '%s' END processing: %s%n", id, message.payload());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }
}


public class Main {
    public static void main(String[] args) throws InterruptedException {
        // 1. Setup Broker and Topics
        BrokerImpl broker = new BrokerImpl();
        broker.createTopic("finance");
        broker.createTopic("engineering");

        // 2. Setup Producers
        IProducer financeProducer = new Producer(broker);
        IProducer engineeringProducer = new Producer(broker);

        // 3. Setup Consumers with their async processing logic
        // Engineering Consumer Group (2 consumers)
        IConsumer engConsumer1 = new EngineeringConsumer("EngConsumer-1");
        IConsumer engConsumer2 = new EngineeringConsumer("EngConsumer-2");
        broker.subscribe("engineering", "EngGroup", engConsumer1);
        broker.subscribe("engineering", "EngGroup", engConsumer2);

        // Finance Consumer Group (1 consumer)
        IConsumer finConsumer1 = new FinanceConsumer("FinConsumer-1");
        broker.subscribe("finance", "FinGroup", finConsumer1);

        // Another group on the engineering topic
        IConsumer engAuditor = new EngineeringConsumer("EngAuditor-1");
        broker.subscribe("engineering", "AuditGroup", engAuditor);


        // 4. Start producing messages asynchronously
        System.out.println("--- Starting Producers ---");
        for (int i = 0; i < 10; i++) {
            final int msgNum = i;
            financeProducer.send(new Message("finance", "Payment #" + msgNum))
                    .whenComplete((receipt, ex) -> {
                        if (ex == null) System.out.println("Producer ACK: Finance message sent, ID: " + receipt.messageId());
                    });

            engineeringProducer.send(new Message("engineering", "Build #" + msgNum))
                    .whenComplete((receipt, ex) -> {
                        if (ex == null) System.out.println("Producer ACK: Engineering message sent, ID: " + receipt.messageId());
                    });

            Thread.sleep(50); // Simulate producers sending at different times
        }

        // 5. Let the system run for a while to process messages
        System.out.println("\n--- System running, consumers processing... ---\n");
        TimeUnit.SECONDS.sleep(10);

        // 6. Initiate graceful shutdown
        broker.shutdown();
    }
}