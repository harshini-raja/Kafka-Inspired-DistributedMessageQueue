package com.distributedQueue.Core;

import com.distributedQueue.Metadata.PartitionMetadata;

import java.util.HashMap;
import java.util.Map;

public class Topic {
    private final String name;
    private final int replicationFactor;
    private final Map<Integer, Partition> partitions = new HashMap<>();
    private final Map<Integer, PartitionMetadata> partitionsMetadata = new HashMap<>();

    public Topic(String name, int numPartitions, int replicationFactor) {
        this.name = name;
        this.replicationFactor = replicationFactor;

        for (int i = 0; i < numPartitions; i++) {
            Partition partition = new Partition(i, replicationFactor);
            partitions.put(i, partition);
        }
    }

    public String getName() {
        return name;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public Map<Integer, Partition> getPartitions() {
        return partitions;
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    public PartitionMetadata getPartitionMetadata(int partitionId) {
        return partitionsMetadata.get(partitionId);
    }

    public void updatePartitionMetadata(int partitionId, PartitionMetadata metadata) {
        partitionsMetadata.put(partitionId, metadata);
    }

    public int getNumPartitions() {
        return partitions.size();
    }
}
