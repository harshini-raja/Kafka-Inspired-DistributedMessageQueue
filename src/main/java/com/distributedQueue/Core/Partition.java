package com.distributedQueue.Core;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.distributedQueue.Metadata.PartitionMetadata;

public class Partition implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int partitionId;
    private final int replicationFactor;
    private PartitionMetadata partitionMetadata;

    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger offset = new AtomicInteger(0);
    private boolean isLeader;

    public int getPartitionId() {
        return partitionId;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public Partition(int partitionId, int replicationFactor) {
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
    }

    public void setLeader(boolean isLeader) {
        this.isLeader = isLeader;
    }

    public PartitionMetadata getPartitionMetadata() {
        return partitionMetadata;
    }

    public void setPartitionMetadata(PartitionMetadata partitionMetadata) {
        this.partitionMetadata = partitionMetadata;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public void addMessage(Message message) {
        messageQueue.add(message);
        offset.incrementAndGet();
        // No replication here; handled externally
    }

    public List<Message> getMessages(long startOffset) {
        List<Message> messages = new ArrayList<>(messageQueue);
        long messagesSize = messages.size();
        if (startOffset >= messagesSize) {
            return new ArrayList<>();
        }
        return messages.subList((int)startOffset, messages.size());
    }

    public int getCurrentOffset() {
        return offset.get();
    }
}
