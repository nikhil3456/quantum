package io.github.nikhil3456;

import java.time.Instant;

public record Receipt(String messageId, long timestamp) {
    public Receipt(String messageId) {
        this(messageId, Instant.now().toEpochMilli());
    }
}
