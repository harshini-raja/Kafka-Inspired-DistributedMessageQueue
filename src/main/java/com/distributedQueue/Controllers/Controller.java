package com.distributedQueue.Controllers;

import com.distributedQueue.Logging.LogMessage;
import com.distributedQueue.Logging.LogRepository;
import com.distributedQueue.Metadata.PartitionMetadata;
import com.google.gson.Gson;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Controller {

    private final int controllerPort;
    private final Map<Integer, BrokerInfo> brokerRegistry = new ConcurrentHashMap<>();
    private final Map<Integer, Long> brokerHeartbeats = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> metadata = new ConcurrentHashMap<>();
    private int registeredBrokerCount = 0;
    private final int expectedBrokerCount = 3; // Set this based on your setup
    private final long heartbeatInterval = 2000L; // Expected heartbeat interval in milliseconds
    private final long heartbeatTimeout = 5000L; // Timeout to consider a broker as failed
    private int requestCount = 0; // Counter for requests handled
    private long startTime = System.currentTimeMillis(); // Start time for throughput calculation
    private long totalLatency = 0; // Total latency for calculating average latency

    public Controller(int port) {
        this.controllerPort = port;
        // Start the heartbeat monitoring task
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                this::checkBrokerHeartbeats, heartbeatInterval, heartbeatInterval, TimeUnit.MILLISECONDS);
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(controllerPort), 0);
        server.createContext("/registerBroker", new RegisterBrokerHandler());
        server.createContext("/heartbeat", new HeartbeatHandler());
        server.createContext("/createTopic", new CreateTopicHandler());
        server.createContext("/getMetadata", new GetMetadataHandler());
        server.createContext("/getBrokerInfo", new GetBrokerInfoHandler());
        server.createContext("/getAllBrokers", new GetAllBrokersHandler());
        server.createContext("/getRegisteredBrokerCount", new GetRegisteredBrokerCountHandler());
        server.createContext("/brokers/active", new ActiveBrokersHandler());
        server.createContext("/brokers/active/count", new ActiveBrokerCountHandler());
        server.createContext("/readiness", new ReadinessHandler());
        server.createContext("/logs/controller/stream", new ControllerLogsStreamHandler());

        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        // Add shutdown hook for graceful server shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.stop(0); // Graceful shutdown
        }));
    }

    // Handlers for HTTP requests

    class RegisterBrokerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            long startTime = System.currentTimeMillis();
            requestCount++; // Increment the request count
            BufferedReader in = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
            String line;
            StringBuilder body = new StringBuilder();
            while ((line = in.readLine()) != null) {
                body.append(line);
            }
            in.close();

            String[] params = body.toString().split("&");
            int brokerId = Integer.parseInt(params[0].split("=")[1]);
            String host = params[1].split("=")[1];
            int port = Integer.parseInt(params[2].split("=")[1]);

            BrokerInfo brokerInfo = new BrokerInfo(host, port, brokerId, System.currentTimeMillis());
            registerBroker(brokerId, brokerInfo);
            registeredBrokerCount++; // Increment the counter
            String response = "Broker registered";
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            long endTime = System.currentTimeMillis();
            String logMessage = "Registered broker " + brokerId + " at " + host + ":" + port + " in "
                    + (endTime - startTime) + " ms";
            LogRepository.addLog("Controller", logMessage);
            logLatency(startTime); // Log latency
            logThroughput(); // Log throughput
            os.close();

            // System.out.println("Registered broker " + brokerId + " at " + host + ":" +
            // port);
        }
    }

    class HeartbeatHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String brokerIdStr = exchange.getRequestURI().getQuery().split("=")[1];
            int brokerId = Integer.parseInt(brokerIdStr);
            receiveHeartbeat(brokerId);

            String response = "Heartbeat received";
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            // logLatency(startTime); // Log latency
            // logThroughput(); // Log throughput
            os.close();
        }
    }

    class CreateTopicHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            long startTime = System.currentTimeMillis();
            BufferedReader in = new BufferedReader(new InputStreamReader(exchange.getRequestBody()));
            String line;
            StringBuilder body = new StringBuilder();
            while ((line = in.readLine()) != null) {
                body.append(line);
            }
            in.close();

            String[] params = body.toString().split("&");
            String topicName = params[0].split("=")[1];
            int numPartitions = Integer.parseInt(params[1].split("=")[1]);
            int replicationFactor = Integer.parseInt(params[2].split("=")[1]);

            String response;
            int statusCode;

            boolean success = createTopic(topicName, numPartitions, replicationFactor);
            if (success) {
                response = "Topic created successfully";
                statusCode = 200;
            } else {
                response = "Failed to create topic";
                statusCode = 400; // Bad Request
            }

            exchange.sendResponseHeaders(statusCode, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

            if (success) {
                System.out.println("Topic " + topicName + " created with " + numPartitions + " partitions");
                long endTime = System.currentTimeMillis();

                String logMessage = "Topic " + topicName + " created with " + numPartitions + " partitions in "
                        + (endTime - startTime) + " ms";
                LogRepository.addLog("Controller", logMessage);
                String logMessage1 = "Topic creation took " + (endTime - startTime) + " ms";
                LogRepository.addLog("Controller", logMessage1);
            }

            logLatency(startTime); // Log latency
            logThroughput(); // Log throughput
        }
    }

    class GetMetadataHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response;
            int statusCode;
            long startTime = System.currentTimeMillis();
            String topicName = null;

            try {
                // Extract topic name from query
                String query = exchange.getRequestURI().getQuery();
                topicName = query.split("=")[1];

                // Retrieve topic metadata
                Map<Integer, PartitionMetadata> topicMetadata = metadata.get(topicName);

                if (topicMetadata != null) {
                    Gson gson = new Gson();
                    response = gson.toJson(topicMetadata); // Serialize metadata to JSON
                    statusCode = 200;
                    // System.out.println("Metadata fetched for topic: " + topicName);
                } else {
                    response = "{\"error\": \"No metadata found for topic " + topicName + "\"}";
                    statusCode = 404;
                    // System.err.println("No metadata found for topic: " + topicName);
                }
            } catch (Exception e) {
                response = "{\"error\": \"Invalid request format or processing error.\"}";
                statusCode = 400;
                System.err.println("Error processing metadata request: " + e.getMessage());
            }

            // Send response headers and body
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(statusCode, response.getBytes().length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
                // logLatency(startTime); // Log latency
                // logThroughput(); // Log throughput
            }
        }
    }

    class GetBrokerInfoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String brokerIdStr = exchange.getRequestURI().getQuery().split("=")[1];
            int brokerId = Integer.parseInt(brokerIdStr);
            BrokerInfo brokerInfo = brokerRegistry.get(brokerId);

            String response;
            if (brokerInfo != null) {
                response = brokerInfo.getHost() + ":" + brokerInfo.getPort();
            } else {
                response = "Broker not found";
            }
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }

    class GetAllBrokersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            List<BrokerInfo> brokers = new ArrayList<>(brokerRegistry.values());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(brokers);
            oos.flush();

            byte[] response = baos.toByteArray();
            exchange.sendResponseHeaders(200, response.length);
            OutputStream os = exchange.getResponseBody();
            os.write(response);
            os.close();
        }
    }

    // Handler for /brokers/active endpoint
    class ActiveBrokersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                // Add CORS headers
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
                exchange.getResponseHeaders().set("Content-Type", "application/json");

                try {
                    // Fetch all brokers using the logic from GetAllBrokersHandler
                    List<BrokerInfo> allBrokers = getAllBrokers();

                    // You can filter active brokers based on your logic (example: based on status)
                    List<BrokerInfo> activeBrokers = getActiveBrokers(allBrokers);

                    // Convert active brokers to JSON
                    String response = new Gson().toJson(activeBrokers);
                    exchange.sendResponseHeaders(200, response.getBytes().length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    private List<BrokerInfo> getAllBrokers() {
        return new ArrayList<>(brokerRegistry.values());
    }

    private List<BrokerInfo> getActiveBrokers(List<BrokerInfo> allBrokers) {
        List<BrokerInfo> activeBrokers = new ArrayList<>();
        long currentTime = System.currentTimeMillis();

        for (BrokerInfo brokerInfo : allBrokers) {
            int brokerId = brokerInfo.getBrokerId();

            // Fetch the last heartbeat from the separate map
            long lastHeartbeat = brokerHeartbeats.getOrDefault(brokerId, 0L);

            if (currentTime - lastHeartbeat < heartbeatTimeout) {
                activeBrokers.add(brokerInfo);
            }
        }

        return activeBrokers;
    }

    private class GetRegisteredBrokerCountHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equals(exchange.getRequestMethod())) {
                String response = "{\"registeredBrokerCount\": " + registeredBrokerCount + "}";
                exchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    // Handler for /brokers/active/count endpoint
    class ActiveBrokerCountHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                // Add CORS headers
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
                exchange.getResponseHeaders().set("Content-Type", "application/json");

                try {
                    // Fetch all brokers
                    List<BrokerInfo> allBrokers = getAllBrokers();

                    // Filter active brokers
                    List<BrokerInfo> activeBrokers = getActiveBrokers(allBrokers);

                    // Prepare response with count
                    String response = "{\"activeBrokerCount\": " + activeBrokers.size() + "}";
                    exchange.sendResponseHeaders(200, response.getBytes().length);

                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    class ReadinessHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            boolean ready = isControllerReady();
            String response = ready ? "Ready" : "Not Ready";
            int statusCode = ready ? 200 : 503; // HTTP 200 if ready, 503 if not

            exchange.sendResponseHeaders(statusCode, response.length());
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();

            // System.out.println("Readiness check: " + response);
        }
    }

    public boolean isControllerReady() {
        return brokerRegistry.size() >= expectedBrokerCount;
    }

    // BrokerInfo class to store broker's network information
    public static class BrokerInfo {
        private final String host;
        private final int port;
        private final int brokerId;
        private final long lastHeartbeat;

        public BrokerInfo(String host, int port, int brokerId, long lastHeartbeat) {
            this.host = host;
            this.port = port;
            this.brokerId = brokerId;
            this.lastHeartbeat = lastHeartbeat;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public int getBrokerId() {
            return brokerId;
        }

        public long getLastHeartbeat() {
            return lastHeartbeat;
        }
    }

    // Existing methods...

    public void registerBroker(int brokerId, BrokerInfo brokerInfo) {
        if (brokerRegistry.containsKey(brokerId)) {
            System.out.println("Broker " + brokerId + " is already registered.");
            return;
        }

        brokerRegistry.put(brokerId, brokerInfo);
        System.out.println("Broker " + brokerId + " registered with host " + brokerInfo.getHost() + " and port "
                + brokerInfo.getPort());

        // Additional broker-related logic (e.g., assigning leader/follower roles)
        for (String topicName : metadata.keySet()) {
            Map<Integer, PartitionMetadata> partitionMetadataMap = metadata.get(topicName);
            for (PartitionMetadata partitionMetadata : partitionMetadataMap.values()) {
                boolean leadershipChanged = false;
                if (partitionMetadata.getLeaderId() == -1) {
                    partitionMetadata.setLeaderId(brokerId);
                    leadershipChanged = true;
                    System.out.println("Assigned broker " + brokerId + " as leader for topic " + topicName
                            + " partition " + partitionMetadata.getPartitionId());
                } else if (!partitionMetadata.getFollowers().contains(brokerId)) {
                    partitionMetadata.addFollower(brokerId);
                    System.out.println("Added broker " + brokerId + " as follower for topic " + topicName
                            + " partition " + partitionMetadata.getPartitionId());
                }

                if (leadershipChanged) {
                    notifyBrokerOfLeadershipChange(brokerInfo, topicName, partitionMetadata);
                }
            }
        }

        // System.out.println("Current broker count: " + brokerRegistry.size());
        if (isControllerReady()) {
            System.out.println("Controller is ready: All brokers registered.");
        }
    }

    public void receiveHeartbeat(int brokerId) {
        brokerHeartbeats.put(brokerId, System.currentTimeMillis());
    }

    private void checkBrokerHeartbeats() {
        long currentTime = System.currentTimeMillis();
        for (Integer brokerId : new HashSet<>(brokerRegistry.keySet())) {
            long lastHeartbeat = brokerHeartbeats.getOrDefault(brokerId, 0L);
            if (currentTime - lastHeartbeat > heartbeatTimeout) {
                System.out.println("Broker " + brokerId + " is considered dead.");
                handleBrokerFailure(brokerId);
            }
        }
    }

    private void handleBrokerFailure(int failedBrokerId) {
        // Remove the failed broker from the registry
        brokerRegistry.remove(failedBrokerId);

        // Reassign leadership for partitions led by the failed broker
        for (String topicName : metadata.keySet()) {
            Map<Integer, PartitionMetadata> partitionMetadataMap = metadata.get(topicName);
            for (PartitionMetadata partitionMetadata : partitionMetadataMap.values()) {
                if (partitionMetadata.getLeaderId() == failedBrokerId) {
                    electNewLeader(topicName, partitionMetadata, failedBrokerId);
                }
            }
        }
    }

    private void electNewLeader(String topicName, PartitionMetadata partitionMetadata, int failedBrokerId) {
        List<Integer> followers = partitionMetadata.getFollowers();
        // Remove the failed broker from the followers list if present
        followers.remove(Integer.valueOf(failedBrokerId));

        if (!followers.isEmpty()) {
            // Choose the first follower as the new leader
            int newLeaderId = followers.get(0);
            partitionMetadata.setLeaderId(newLeaderId);

            // Update followers list
            List<Integer> newFollowers = new ArrayList<>(followers);
            newFollowers.remove(Integer.valueOf(newLeaderId));
            partitionMetadata.setFollowers(newFollowers);

            System.out.println("New leader for topic " + topicName + " partition " + partitionMetadata.getPartitionId()
                    + " is broker " + newLeaderId);

            // Notify the new leader broker
            BrokerInfo newLeaderBroker = brokerRegistry.get(newLeaderId);
            if (newLeaderBroker != null) {
                notifyBrokerOfLeadershipChange(newLeaderBroker, topicName, partitionMetadata);
            }
        } else {
            System.out.println("No available brokers to become leader for topic " + topicName + " partition "
                    + partitionMetadata.getPartitionId());
        }
    }

    private void notifyBrokerOfLeadershipChange(BrokerInfo brokerInfo, String topicName,
                                                PartitionMetadata partitionMetadata) {
        try {
            URL url = new URL("http://" + brokerInfo.getHost() + ":" + brokerInfo.getPort() + "/updateLeadership");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            // Send topicName and partitionMetadata
            ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream());
            out.writeObject(topicName);
            out.writeObject(partitionMetadata);
            out.flush();
            out.close();

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Notified broker " + partitionMetadata.getLeaderId() + " of leadership change.");
            } else {
                System.err.println(
                        "Failed to notify broker " + partitionMetadata.getLeaderId() + " of leadership change.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Topic creation method
    public boolean createTopic(String topicName, int numPartitions, int replicationFactor) {
        int retries = 5;
        int delay = 2000; // 2 second

        for (int i = 0; i < retries; i++) {
            if (isControllerReady()) {
                break;
            }
            // System.err.println("Controller not ready. Retrying... (" + (i + 1) + "/" +
            // retries + ")");
            try {
                Thread.sleep(delay);
                delay *= 2; // Exponential backoff
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Retry interrupted.");
                return false;
            }
        }

        if (!isControllerReady()) {
            System.err.println("Cannot create topic: Controller is still not ready after retries.");
            return false;
        }

        if (metadata.containsKey(topicName)) {
            System.err.println("Topic already exists: " + topicName);
            return false;
        }

        if (brokerRegistry.isEmpty()) {
            System.err.println("No brokers registered. Cannot create topic.");
            return false;
        }

        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();

        // Partition and replication logic
        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            PartitionMetadata partitionMetadata = new PartitionMetadata(partitionId, replicationFactor);

            List<Integer> brokerIds = new ArrayList<>(brokerRegistry.keySet());
            int leaderIndex = partitionId % brokerIds.size();
            int leaderId = brokerIds.get(leaderIndex);
            partitionMetadata.setLeaderId(leaderId);

            // Add followers, ensuring no duplicates and no self-replication
            for (int i = 1; i < replicationFactor && i < brokerIds.size(); i++) {
                int followerId = brokerIds.get((leaderIndex + i) % brokerIds.size());
                if (followerId != leaderId && !partitionMetadata.getFollowerIds().contains(followerId)) {
                    partitionMetadata.addFollower(followerId);
                }
            }
            partitionMetadataMap.put(partitionId, partitionMetadata);
        }
        metadata.put(topicName, partitionMetadataMap);
        System.out.println("Topic " + topicName + " created with metadata: " + partitionMetadataMap);

        /**
         * Partition 0:
         * Leader: Broker1.
         * Follower: Broker2 (next broker in the round-robin sequence).
         * Partition 1:
         * Leader: Broker2.
         * Follower: Broker3.
         * Partition 2:
         * Leader: Broker3.
         * Follower: Broker1.
         *
         */

        // Notify brokers of their leadership
        notifyBrokersOfNewTopic(topicName, partitionMetadataMap);

        return true;
    }

    private void notifyBrokersOfNewTopic(String topicName, Map<Integer, PartitionMetadata> partitionMetadataMap) {
        for (PartitionMetadata partitionMetadata : partitionMetadataMap.values()) {
            int leaderId = partitionMetadata.getLeaderId();
            if (leaderId != -1) {
                BrokerInfo leaderBroker = brokerRegistry.get(leaderId);
                if (leaderBroker != null) {
                    notifyBrokerOfLeadershipChange(leaderBroker, topicName, partitionMetadata);
                }
            }
            for (int followerId : partitionMetadata.getFollowers()) {
                BrokerInfo followerBroker = brokerRegistry.get(followerId);
                if (followerBroker != null) {
                    notifyBrokerOfLeadershipChange(followerBroker, topicName, partitionMetadata);
                }
            }
        }
    }

    private void logThroughput() {
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        if (elapsedTime > 0) {
            double throughput = (requestCount * 1000.0) / elapsedTime; // Requests per second
            String logMessage = "Throughput: " + throughput + " requests/second";
            LogRepository.addLog("Controller", logMessage);
        }
    }

    private void logLatency(long startTime) {
        long endTime = System.currentTimeMillis();
        long latency = endTime - startTime;
        totalLatency += latency;
        double averageLatency = totalLatency / (double) requestCount;
        String logMessage = "Request Latency: " + latency + " ms (Average: " + averageLatency + " ms)";
        LogRepository.addLog("Controller", logMessage);
    }

    class ControllerLogsStreamHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // Set CORS headers to allow requests from any origin (or specify your frontend URL for security)
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET");
            exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

            // Set the response type to event-stream for SSE
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);

            OutputStream os = exchange.getResponseBody();

            // Keep the connection open and send log data periodically
            while (true) {
                // Get the logs for the controller (you can modify this to get logs from other components if needed)
                List<LogMessage> controllerLogs = LogRepository.getLogsBySource("Controller");

                // Map LogMessages to a JSON structure
                List<String> logMessages = controllerLogs.stream()
                        .map(log -> {
                            // Create a JSON object for each log
                            return String.format("{\"timestamp\": \"%s\", \"message\": \"%s\"}",
                                    log.getTimestamp(), log.getMessage());
                        })
                        .collect(Collectors.toList());

                // If there are new logs, send them to the client via SSE
                if (!logMessages.isEmpty()) {
                    // Join the logs into a single string, one per line, with each log prefixed by "data:"
                    String response = logMessages.stream()
                            .map(log -> "data: " + log + "\n\n") // Format as SSE event
                            .collect(Collectors.joining());
                    os.write(response.getBytes());
                    os.flush();
                }

                // Sleep for a while before sending new logs (simulate waiting for new logs)
                try {
                    Thread.sleep(10000); // Wait for 10 seconds before sending new data
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }


}
