package com.distributedQueue.Consumers;

import com.distributedQueue.Adapters.MessageAdapter;
import com.distributedQueue.Core.Message;
import com.distributedQueue.Logging.LogMessage;
import com.distributedQueue.Logging.LogRepository;
import com.distributedQueue.Metadata.PartitionMetadata;
import com.distributedQueue.Producers.Producer.BrokerInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import java.util.ArrayList;
import java.util.Comparator;

public class Consumer {

    private final String controllerHost;
    private final int controllerPort;
    private final Set<String> consumedMessageKeys = ConcurrentHashMap.newKeySet(); // track consumed messages based on
    // composite keys
    private final int maxMessages = 35; // Maximum number of messages to consume
    private int messageCount = 0; // Counter for messages consumed
    private long startTime = System.currentTimeMillis(); // Start time for throughput calculation
    private long totalLatency = 0; // Total latency for calculating average latency

    private static final List<String> logMessages = new ArrayList<>();

    public Consumer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        // Start the HTTP server
        startHttpServer();
    }

    private static void startHttpServer() {
        try {
            int port = 8083; // Choose an available port for the consumer
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/logs", new LogsHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            System.out.println("Consumer HTTP server started on port " + port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Handler to serve logs
    static class LogsHandler implements HttpHandler {
        private static final Gson gson = new Gson();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // Add CORS headers
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

            // Convert log messages to JSON
            String jsonResponse = gson.toJson(logMessages);

            // Send the response
            exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(jsonResponse.getBytes());
            os.close();
        }
    }

    // Modify your logging methods to add logs to logMessages
    private void log(String message) {
        System.out.println(message);
        synchronized (logMessages) {
            logMessages.add(message);
        }
    }

    public void consume(String topic) {
        // Wait for the controller and metadata to be ready
        if (!waitForReadiness()) {
            System.err.println("Controller not ready. Exiting consume operation.");
            return;
        }

        long lastOffset = 0; // Start from the beginning of the partition
        int consumedMessages = 0; // Counter for the number of consumed messages
        while (consumedMessages < maxMessages) {
            try {
                // Fetch metadata for the topic
                Map<Integer, PartitionMetadata> topicMetadata = fetchMetadataWithRetries(topic);
                if (topicMetadata == null) {
                    System.err.println("Topic metadata not found for topic " + topic);
                    Thread.sleep(5000); // Retry after delay
                    continue;
                }

                boolean success = false;

                // Try fetching messages from any available partition
                for (Map.Entry<Integer, PartitionMetadata> entry : topicMetadata.entrySet()) {
                    int partitionId = entry.getKey();
                    PartitionMetadata partitionMetadata = entry.getValue();

                    // Attempt to fetch messages from the leader broker first
                    long updatedOffset = fetchMessagesFromBroker(partitionMetadata, topic, partitionId, lastOffset);
                    if (updatedOffset != lastOffset) {
                        // If offset was updated, update the lastOffset and mark success
                        lastOffset = updatedOffset;
                        success = true;
                    }

                    if (!success) {
                        // If leader fails, try the followers
                        List<Integer> followers = partitionMetadata.getFollowerIds();
                        for (int followerId : followers) {
                            BrokerInfo followerInfo = fetchBrokerInfo(followerId);
                            if (followerInfo != null) {
                                System.out.println(
                                        "Attempting to consume from follower broker: " + followerInfo.getHost());
                                updatedOffset = fetchMessages(followerInfo, topic, partitionId, lastOffset);
                                if (updatedOffset != lastOffset) {
                                    lastOffset = updatedOffset;
                                    consumedMessages++;
                                    success = true;
                                    break; // Exit the loop if successful
                                }
                            }
                        }
                    }
                    // If successful, break out of the partition loop
                    if (success) {
                        break;
                    }
                }
                // If all partitions fail, refresh metadata and retry
                if (!success) {
                    System.err.println("All brokers failed for topic " + topic + ". Retrying...");
                    Thread.sleep(5000); // Retry after delay
                }
            } catch (Exception e) {
                System.err.println("Error during consumption: " + e.getMessage());
                e.printStackTrace();
                try {
                    Thread.sleep(5000); // Retry after delay
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

   

    private long fetchMessagesFromBroker(PartitionMetadata partitionMetadata, String topic, int partitionId,
                                         long lastOffset) {
        BrokerInfo leaderInfo = fetchBrokerInfo(partitionMetadata.getLeaderId());
        if (leaderInfo == null) {
            log("Leader broker info not found for broker ID " + partitionMetadata.getLeaderId());
            return lastOffset; // Leader broker info unavailable
        }
        return fetchMessages(leaderInfo, topic, partitionId, lastOffset);
    }

    private long fetchMessages(BrokerInfo brokerInfo, String topic, int partitionId, long lastOffset) {
        try {
            // Construct the URL for long-polling from the broker
            URL url = new URL("http://" + brokerInfo.getHost() + ":" + brokerInfo.getPort()
                    + "/longPolling?topicName=" + topic + "&partitionId=" + partitionId
                    + "&offset=" + lastOffset);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(10000); // 10 seconds timeout for connection
            conn.setReadTimeout(200000);

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();
                // Deserialize and process the messages
                Gson gson = new GsonBuilder()
                        .registerTypeAdapter(Message.class, new MessageAdapter())
                        .create();
                List<Message> messages = gson.fromJson(response, new TypeToken<List<Message>>() {
                }.getType());

                if (messages == null || messages.isEmpty()) {
                    log("No new messages available in Partition " + partitionId);
                }

                // Ensure messages are sorted by offset (ascending order)
                messages.sort(Comparator.comparingLong(Message::getOffset));
                for (Message message : messages) {
                    // Generate a composite key for the message
                    String messageKey = generateMessageKey(message);

                    // Process only if the message is not a duplicate
                    if (consumedMessageKeys.add(messageKey)) {
                        log("Consumed message from Partition " + partitionId + ": "
                                + new String(message.getPayload()));
                        lastOffset = message.getOffset(); // Update the offset after processing
                        messageCount++; // Increment the message count
                        logLatency(message.getTimestamp().toEpochMilli()); // Log latency
                    } else {
                        // log("Duplicate message skipped: " + messageKey);
                    }
                }
                return lastOffset; // Return the updated offset
            } else {
                log("Failed to fetch messages from Partition " + partitionId + ". Response Code: "
                        + responseCode);
                return lastOffset; // Failed to fetch messages
            }
        } catch (IOException e) {
            log("Error while fetching messages from broker " + brokerInfo.getHost() + ":"
                    + brokerInfo.getPort() + " for Partition " + partitionId + ": " + e.getMessage());
            return lastOffset; // If an error occurs, return false
        }
    }

    // Helper method to generate a composite key
    private String generateMessageKey(Message message) {
        return message.getTopic() + "|" + message.getPartition() + "|" + message.getOffset() + "|"
                + message.getTimestamp();
    }

    private boolean waitForReadiness() {
        int maxRetries = 10; // Maximum number of retries
        int delay = 2000; // Delay between retries (milliseconds)

        for (int i = 0; i < maxRetries; i++) {
            if (isControllerReady()) {
                return true;
            }
            // log("Controller not ready. Retrying in " + (delay / 1000) + "
            // seconds...");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log("Readiness check interrupted.");
                return false;
            }
        }
        log("Controller did not become ready after multiple attempts.");
        return false;
    }

    private Map<Integer, PartitionMetadata> fetchMetadataWithRetries(String topic) {
        int retries = 5;
        int delay = 1000; // 1 second delay
        while (retries-- > 0) {
            Map<Integer, PartitionMetadata> metadata = fetchTopicMetadata(topic);
            if (metadata != null) {
                return metadata; // Metadata available
            } else {
                // log("Metadata not found for topic " + topic + ".
                // Retrying...");
                try {
                    Thread.sleep(delay);
                    delay *= 2; // Exponential backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        return null; // Return null if metadata could not be fetched
    }

    private Map<Integer, PartitionMetadata> fetchTopicMetadata(String topicName) {
        // Use the existing waitForReadiness method to ensure the controller is ready
        if (!waitForReadiness()) {
            log("Controller is not ready after retries. Exiting metadata fetch.");
            return null;
        }

        // Fetch metadata after confirming controller readiness
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                StringBuilder responseBuilder = new StringBuilder();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        responseBuilder.append(line);
                    }
                }
                String response = responseBuilder.toString();
                // log("Received JSON response: " + response);

                // Deserialize the JSON response to Map<Integer, PartitionMetadata>
                Gson gson = new Gson();
                Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                }.getType();
                return gson.fromJson(response, type);
            } else {
                // log("Failed to fetch metadata for topic " + topicName + ",
                // response code: " + responseCode);
            }
        } catch (IOException e) {
            log("Error fetching metadata for topic " + topicName + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private boolean isControllerReady() {
        String readinessUrl = "http://" + controllerHost + ":" + controllerPort + "/readiness";
        try {
            // Create a connection to the readiness endpoint
            URL url = new URL(readinessUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Controller is ready
                // log("Controller is ready.");
                return true;
            } else if (responseCode == 503) {
                // Controller is not ready yet
                // log("Controller is not ready yet.");
            } else {
                log("Unexpected response code from readiness check: " + responseCode);
            }
        } catch (IOException e) {
            log("Error checking controller readiness: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getBrokerInfo?brokerId=" + brokerId);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String response = in.readLine();
                in.close();

                if (response.equals("Broker not found")) {
                    log("Broker not found for broker ID " + brokerId);
                    return null;
                }

                String[] parts = response.split(":");
                if (parts.length < 2) {
                    log("Invalid broker info format for broker ID " + brokerId + ": " + response);
                    return null;
                }

                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                return new BrokerInfo(host, port);
            } else {
                log(
                        "Failed to fetch broker info for broker ID " + brokerId + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void logThroughput() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        if (elapsedTime > 0) {
            double throughput = (messageCount * 1000.0) / elapsedTime; // Messages per second
            String logMessage = "Throughput: " + throughput + " messages/second";
            log(logMessage);
        }
    }

    public void logLatency(long productionTimestamp) {
        long consumptionTimestamp = System.currentTimeMillis();
        long latency = consumptionTimestamp - productionTimestamp;
        totalLatency += latency;
    }

    public void logAverageLatency() {
        double averageLatency = totalLatency / (double) messageCount;
        String logMessage = "Average End-to-End Latency: " + averageLatency + " ms";
        log(logMessage);
    }

}