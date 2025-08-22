package io.github.nikhil3456;

import java.util.UUID;

public record Message(String id, String topic, String payload) {
    public Message(String topic, String payload) {
        this(UUID.randomUUID().toString(), topic, payload);
    }
}
