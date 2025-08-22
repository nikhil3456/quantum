package io.github.nikhil3456;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class Topic {
    private final String name;
    // private final ConcurrentLinkedQueue<Message> messageQueue;
    private final List<Message> messageStore;
    // why AtomicInteger?
    private final ConcurrentHashMap<String, AtomicInteger> consumerGroupOffsets;

    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Boolean>> messagesInFlightPerGroup;
    private final ReentrantLock messageStoreLock = new ReentrantLock();

    private final ConcurrentHashMap<String, ReentrantLock> groupLocks = new ConcurrentHashMap<>();

    public Topic(String name) {
        this.name = name;
        // this.messageQueue = new ConcurrentLinkedQueue<>();
        this.messageStore = new ArrayList<>();
        this.consumerGroupOffsets = new ConcurrentHashMap<>();
        this.messagesInFlightPerGroup = new ConcurrentHashMap<>();
    }

    public void addMessage(Message message) {
        messageStoreLock.lock();
        try {
            messageStore.add(message);
        } finally {
            messageStoreLock.unlock();
        }
    }

    private ReentrantLock getLockForGroup(String consumerGroupId) {
        return groupLocks.computeIfAbsent(consumerGroupId, k -> new ReentrantLock());
    }

    private ConcurrentHashMap<Integer, Boolean> getInFlightMapForGroup(String consumerGroupId) {
        return messagesInFlightPerGroup.computeIfAbsent(consumerGroupId, k -> new ConcurrentHashMap<>());
    }

    public Message getMessageForConsumer(String consumerGroupId) {
        consumerGroupOffsets.putIfAbsent(consumerGroupId, new AtomicInteger(0));
        ReentrantLock groupLock = getLockForGroup(consumerGroupId);
        ConcurrentHashMap<Integer, Boolean> groupInFlight = getInFlightMapForGroup(consumerGroupId);

        groupLock.lock();
        try {
            int currentOffset = consumerGroupOffsets.get(consumerGroupId).get();
            int storeSize;

            messageStoreLock.lock();
            try {
                storeSize = messageStore.size();
            } finally {
                messageStoreLock.unlock();
            }

            if (currentOffset >= storeSize) {
                return null;
            }

            // Find the next message not in flight
            for (int i=currentOffset;i < storeSize;i++) {
                if (groupInFlight.putIfAbsent(i, true) == null) {
                    return messageStore.get(i);
                }
            }

            return null;
        } finally {
            groupLock.unlock();
        }
    }

    public void commitOffset(String consumerGroupId, String messageId) {
        ReentrantLock groupLock = getLockForGroup(consumerGroupId);
        ConcurrentHashMap<Integer, Boolean> groupInFlight = getInFlightMapForGroup(consumerGroupId);

        groupLock.lock();
        try {
            int offsetToCommit = -1;
            for (int i=0;i<messageStore.size();i++) {
                if (messageStore.get(i).id().equals(messageId)) {
                    offsetToCommit = i;
                    break;
                }
            }

            if (offsetToCommit != -1) {
                consumerGroupOffsets.get(consumerGroupId).set(offsetToCommit+1);
                groupInFlight.remove(offsetToCommit);
            }
        } finally {
            groupLock.unlock();
        }
    }

    public String getName() {
        return name;
    }

}
