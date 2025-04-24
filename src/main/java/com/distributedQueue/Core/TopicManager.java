package com.distributedQueue.Core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.distributedQueue.Brokers.Broker;
import com.distributedQueue.Metadata.PartitionMetadata;

public class TopicManager {

    private final Map<String, Topic> topics = new HashMap<>();
    private final List<Broker> brokers;

    public TopicManager(List<Broker> brokers) {
        this.brokers = brokers;
    }

    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        if (topics.containsKey(topicName)) {
            System.err.println("Topic already exists: " + topicName);
            return;
        }

        Topic topic = new Topic(topicName, numPartitions, replicationFactor);
        topics.put(topicName, topic);

        for (int partition = 0; partition < numPartitions; partition++) {
            PartitionMetadata partitionMetadata = new PartitionMetadata(partition, replicationFactor);

            // Assign leader and followers
            if (!brokers.isEmpty()) {
                int leaderId = brokers.get(partition % brokers.size()).getBrokerId();
                partitionMetadata.setLeaderId(leaderId);

                for (int i = 1; i < replicationFactor && i < brokers.size(); i++) {
                    int followerId = brokers.get((partition + i) % brokers.size()).getBrokerId();
                    partitionMetadata.addFollower(followerId);
                }
            }
            topic.updatePartitionMetadata(partition, partitionMetadata);
        }

        for (Broker broker : brokers) {
            broker.addTopic(topic);
        }

        System.out.println("Topic " + topicName + " with " + numPartitions +
                " partitions and replication factor " + replicationFactor + " created and registered.");
    }

    // Method to retrieve metadata for all partitions of a given topic
    public Map<Integer, PartitionMetadata> getTopicMetadata(String topicName) {
        Topic topic = topics.get(topicName);
        if (topic != null) {
            Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();
            for (int partitionId = 0; partitionId < topic.getNumPartitions(); partitionId++) {
                PartitionMetadata partitionMetadata = topic.getPartitionMetadata(partitionId);
                partitionMetadataMap.put(partitionId, partitionMetadata);
            }
            return partitionMetadataMap;
        }
        return null;
    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    // Method to get all topic names
    public Set<String> getTopicNames() {
        return topics.keySet();
    }

}

