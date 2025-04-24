package com.distributedQueue.Metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PartitionMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    private int partitionId;
    private int leaderId = -1; // Store the leader by ID rather than Broker object
    private final List<Integer> followerIds = new ArrayList<>(); // Store followers by their broker IDs

    public List<Integer> getFollowerIds() {
        return followerIds;
    }

    // Default constructor needed for Gson deserialization
    public PartitionMetadata() {
    }

    public PartitionMetadata(int partitionId, int replicationFactor) {
        this.partitionId = partitionId;
        // Optionally, initialize other replication-related fields
    }

    public int getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }

    public List<Integer> getFollowers() {
        return new ArrayList<>(followerIds); // Defensive copy
    }

    public void setFollowers(List<Integer> followerIds) {
        this.followerIds.clear();
        this.followerIds.addAll(followerIds);
    }

    public void addFollower(int followerId) {
        this.followerIds.add(followerId);
    }

    @Override
    public String toString() {
        return "PartitionMetadata{" +
                "partitionId=" + partitionId +
                ", leaderId=" + leaderId +
                ", followerIds=" + followerIds +
                '}';
    }
}
