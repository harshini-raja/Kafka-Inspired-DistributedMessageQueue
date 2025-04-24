package com.distributedQueue.Adapters;

import com.distributedQueue.Core.Message;
import com.google.gson.*;
import java.lang.reflect.Type;
import java.time.Instant;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {

    @Override
    public JsonElement serialize(Message message, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject jsonObject = new JsonObject();

        // Serialize regular fields
        jsonObject.add("offset", new JsonPrimitive(message.getOffset()));
        jsonObject.add("payload", message.getPayload() != null ? new JsonPrimitive(new String(message.getPayload())) : JsonNull.INSTANCE);
        jsonObject.add("timestamp", new JsonPrimitive(message.getTimestamp().toString()));
        jsonObject.add("topic", message.getTopic() != null ? new JsonPrimitive(message.getTopic()) : JsonNull.INSTANCE);
        jsonObject.add("partition", new JsonPrimitive(message.getPartition()));

        // Serialize gossip-related fields
        jsonObject.add("senderId", message.getSenderId() != null ? new JsonPrimitive(message.getSenderId()) : JsonNull.INSTANCE);
        jsonObject.add("gossipTimestamp", message.getGossipTimestamp() != null ? new JsonPrimitive(message.getGossipTimestamp().toString()) : JsonNull.INSTANCE);
        jsonObject.add("gossipMetadata", message.getGossipMetadata() != null ? new JsonPrimitive(message.getGossipMetadata()) : JsonNull.INSTANCE);

        return jsonObject;
    }

    @Override
    public Message deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject jsonObject = json.getAsJsonObject();

        // Deserialize regular fields
        long offset = jsonObject.get("offset").getAsLong();
        byte[] payload = jsonObject.has("payload") && !jsonObject.get("payload").isJsonNull()
                ? jsonObject.get("payload").getAsString().getBytes()
                : null;
        Instant timestamp = Instant.parse(jsonObject.get("timestamp").getAsString());
        String topic = jsonObject.has("topic") && !jsonObject.get("topic").isJsonNull()
                ? jsonObject.get("topic").getAsString()
                : null;
        int partition = jsonObject.has("partition") ? jsonObject.get("partition").getAsInt() : -1;

        // Deserialize gossip-related fields
        String senderId = jsonObject.has("senderId") && !jsonObject.get("senderId").isJsonNull()
                ? jsonObject.get("senderId").getAsString()
                : null;
        Instant gossipTimestamp = jsonObject.has("gossipTimestamp") && !jsonObject.get("gossipTimestamp").isJsonNull()
                ? Instant.parse(jsonObject.get("gossipTimestamp").getAsString())
                : null;
        String gossipMetadata = jsonObject.has("gossipMetadata") && !jsonObject.get("gossipMetadata").isJsonNull()
                ? jsonObject.get("gossipMetadata").getAsString()
                : null;

        if (senderId != null) {
            // If gossip-related fields are present, create a gossip message
            return new Message(senderId, gossipMetadata, gossipTimestamp);
        } else {
            // Create a new regular message with the deserialized fields
            return new Message(topic, partition, offset, payload, timestamp);
        }
    }
}