package io.github.nikhil3456;

import java.util.concurrent.CompletableFuture;

public interface IProducer {
    CompletableFuture<Receipt> send(Message message);
}
