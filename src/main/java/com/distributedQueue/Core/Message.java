package com.distributedQueue.Core;

import com.google.gson.Gson;

import java.io.Serializable;
import java.time.Instant;

public class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private final long offset; // offsets are globally unique and are partition-specific
    private final byte[] payload;
    private final Instant timestamp;
    private final String topic;
    private final int partition;

    // Gossip-related fields
    private String senderId;      // ID of the node that sent this gossip
    private Instant gossipTimestamp; // Timestamp of the gossip message
    private String gossipMetadata;  // Metadata about the gossip (partition/topic state, etc.)

    // Regular message constructor
    public Message(String topic, int partition, long offset, byte[] payload) {
        this.offset = offset;
        this.payload = payload;
        this.timestamp = Instant.now();
        this.topic = topic;
        this.partition = partition;
    }

    // Gossip message constructor
    public Message(String senderId, String gossipMetadata, Instant gossipTimestamp) {
        this.offset = -1; // Offset is not used for gossip messages
        this.payload = null; // No payload for gossip messages
        this.timestamp = Instant.now(); // The time this gossip message was created
        this.topic = null; // Not used for gossip
        this.partition = -1; // Not used for gossip
        this.senderId = senderId;
        this.gossipTimestamp = gossipTimestamp;
        this.gossipMetadata = gossipMetadata;
    }

    // Constructor for messages with an existing timestamp
    public Message(String topic, int partition, long offset, byte[] payload, Instant timestamp) {
        this.offset = offset;
        this.payload = payload;
        this.timestamp = timestamp != null ? timestamp : Instant.now();
        this.topic = topic;
        this.partition = partition;
    }

    // Getters for regular message fields
    public long getOffset() {
        return offset;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    // Getters for gossip-related fields
    public String getSenderId() {
        return senderId;
    }

    public Instant getGossipTimestamp() {
        return gossipTimestamp;
    }

    public String getGossipMetadata() {
        return gossipMetadata;
    }

    // Method to serialize the Message object to JSON using Gson
    public String serializeMessage() {
        Gson gson = new Gson();
        return gson.toJson(this); // Serialize the current object into JSON format
    }

    // Method to convert the message to a String representation (for printing)
    @Override
    public String toString() {
        if (gossipMetadata != null) {
            return "GossipMessage{" +
                    "senderId='" + senderId + '\'' +
                    ", gossipMetadata='" + gossipMetadata + '\'' +
                    ", gossipTimestamp=" + gossipTimestamp +
                    ", timestamp=" + timestamp + '}';
        } else {
            return "Message{" +
                    "offset=" + offset +
                    ", topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", timestamp=" + timestamp + '}';
        }
    }

    // Method to create a Message object from JSON using Gson
    public static Message fromJson(String json) {
        Gson gson = new Gson();
        return gson.fromJson(json, Message.class); // Deserialize the JSON string back to a Message object
    }
}