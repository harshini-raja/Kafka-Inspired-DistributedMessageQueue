package com.distributedQueue.Producers;

import com.distributedQueue.Core.Message;
import com.distributedQueue.Logging.LogMessage;
import com.distributedQueue.Logging.LogRepository;
import com.distributedQueue.Metadata.PartitionMetadata;
import com.distributedQueue.Producers.Producer.BrokerInfo;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

public class Producer {

    private static final int LOG_RATE_CONTROL = 25;
    private final String controllerHost;
    private final int controllerPort;

    private Map<String, Integer> partitionCounter = new ConcurrentHashMap<>(); // track the current partition for
    // round-robin
    private Map<String, Map<Integer, Long>> partitionOffsetMap = new ConcurrentHashMap<>(); // track the current offset
    private int messageCount = 0; // Counter for messages produced
    // for each partition
    private long startTime = System.currentTimeMillis(); // Start time for throughput calculation
    private long totalLatency = 0; // Total latency for calculating average latency

    private static final List<String> logMessages = new ArrayList<>();

    public Producer(String controllerHost, int controllerPort) {
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        // Start the HTTP server
        startHttpServer();
    }

    private static void startHttpServer() {
        try {
            int port = 8082; // Choose an available port for the producer
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/logs", new LogsHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            System.out.println("Producer HTTP server started on port " + port);
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

    public void send(String topic, byte[] payload) {
        // Ensure the controller and brokers are ready
        if (!waitForReadiness()) {
            log("Controller or brokers are not ready. Aborting send operation.");
            return;
        }

        // Fetch metadata with retry logic
        Map<Integer, PartitionMetadata> topicMetadata = null;
        int retries = 5;
        int delay = 1000; // 1 second initial delay

        for (int i = 0; i < retries; i++) {
            topicMetadata = fetchMetadata(topic);
            if (topicMetadata != null)
                break;

            log("Retrying to fetch metadata... Attempt " + (i + 1));
            try {
                Thread.sleep(delay);
                delay *= 2; // Exponential backoff
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log("Retry interrupted.");
                return;
            }
        }

        if (topicMetadata == null) {
            log("Failed to fetch metadata after multiple attempts. Aborting send operation.");
            return;
        }

        // Calculate the partition using round-robin
        int numberOfPartitions = topicMetadata.size();
        int partitionId = getNextPartition(topic, numberOfPartitions);

        PartitionMetadata partitionMetadata = topicMetadata.get(partitionId);
        if (partitionMetadata == null) {
            // log("No metadata found for partition " + partitionId);
            return;
        }

        int leaderId = partitionMetadata.getLeaderId();
        BrokerInfo leaderInfo = fetchBrokerInfo(leaderId);
        if (leaderInfo == null) {
            log("Leader broker info not found for broker ID " + leaderId);
            return;
        }

        long offset = getNextOffset(topic, partitionId);
        long productionTimestamp = System.currentTimeMillis();
        Message message = new Message(topic, partitionId, offset, payload);
        // log("Sending message to topic " + topic + ", partition " + partitionId + ", offset " + offset);
        publishMessage(leaderInfo, message);
        logLatency(productionTimestamp); // Log latency
        logThroughput(); // Log throughput

    }

    private long getNextOffset(String topic, int partition) {

        partitionOffsetMap.putIfAbsent(topic, new HashMap<>());
        Map<Integer, Long> offsets = partitionOffsetMap.get(topic);
        offsets.putIfAbsent(partition, 0L);
        long nextOffset = offsets.get(partition);
        offsets.put(partition, nextOffset + 1);
        return nextOffset;
    }

    // Get the next partition ID using a round-robin strategy
    private int getNextPartition(String topic, int numberOfPartitions) {
        // Initialize the counter for the topic if not present
        partitionCounter.putIfAbsent(topic, 0);

        // Get the current partition index and increment it atomically
        int currentPartition = partitionCounter.get(topic);
        int nextPartition = (currentPartition + 1) % numberOfPartitions;

        // Update the counter
        partitionCounter.put(topic, nextPartition);
        return currentPartition;
    }

    private boolean waitForReadiness() {
        int maxRetries = 10; // Maximum number of retries
        int delay = 2000; // Delay between retries (milliseconds)

        for (int i = 0; i < maxRetries; i++) {
            if (isControllerReady()) {
                return true;
            }
            //log("Controller not ready. Retrying in " + (delay / 1000) + " seconds...");
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

    private void publishMessage(BrokerInfo leaderInfo, Message message) {
        try {
            URL url = new URL("http://" + leaderInfo.getHost() + ":" + leaderInfo.getPort() + "/publishMessage");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            try (ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream())) {
                out.writeObject(message);
                out.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // log("Message sent successfully to broker " + leaderInfo.getHost());
                messageCount++;
            } else {
                log("Failed to send message, response code: " + responseCode);
            }
        } catch (IOException e) {
            log("Error publishing message: " + e.getMessage());
            e.printStackTrace();
        }
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
            }
            else if (responseCode == 503) {
                // Controller is not ready
                // log("Controller is not ready.");
            }
            else {
                log("Unexpected response code from readiness check: " + responseCode);
            }
        } catch (IOException e) {
            log("Error checking controller readiness: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    private Map<Integer, PartitionMetadata> fetchMetadata(String topicName) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String response = in.readLine();
                    Gson gson = new Gson();
                    Type mapType = new TypeToken<Map<Integer, PartitionMetadata>>() {
                    }.getType();
                    return gson.fromJson(response, mapType);
                }
            } else {
                // log("Failed to fetch metadata for topic " + topicName + ", response code: " + responseCode);
            }
        } catch (IOException e) {
            log("Error fetching metadata for topic " + topicName + ": " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {
        try {
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getBrokerInfo?brokerId=" + brokerId);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String response = in.readLine();
                    String[] parts = response.split(":");
                    if (parts.length == 2) {
                        return new BrokerInfo(parts[0], Integer.parseInt(parts[1]));
                    }
                }
            } else {
                log("Failed to fetch broker info for broker ID " + brokerId);
            }
        } catch (IOException e) {
            log("Error fetching broker info: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    // Add the createTopic method
    public void createTopic(String topicName, int numPartitions, int replicationFactor) {
        try {
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/createTopic");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            String params = "topicName=" + topicName + "&numPartitions=" + numPartitions + "&replicationFactor="
                    + replicationFactor;
            try (OutputStream os = conn.getOutputStream()) {
                os.write(params.getBytes());
                os.flush();
            }

            if (conn.getResponseCode() == 200) {
                log("Topic " + topicName + " created successfully.");
            } else {
                log("Failed to create topic " + topicName);
            }
        } catch (IOException e) {
            log("Error creating topic: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void logThroughput() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        if (elapsedTime > 0) {
            double throughput = (messageCount * 1000.0) / elapsedTime; // Messages per second
            if (messageCount % LOG_RATE_CONTROL == 1) {
                String logMessage = "Throughput: " + throughput + " messages/second";
                log(logMessage);
            }
        }
    }

    private void logLatency(long productionTimestamp) {
        long consumptionTimestamp = System.currentTimeMillis();
        long latency = consumptionTimestamp - productionTimestamp;
        totalLatency += latency;
        double averageLatency = totalLatency / (double) messageCount;
        if (messageCount % LOG_RATE_CONTROL == 1) {
            String logMessage = "End-to-End Latency: " + latency + " ms (Average: " + averageLatency + " ms)";
            log(logMessage);
        }
    }

    public static class BrokerInfo {
        private final String host;
        private final int port;

        public BrokerInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}