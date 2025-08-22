package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;

public interface IConsumer {
    CompletableFuture<Void> process(Message message);
    String getConsumerId();
}
