package com.distributedQueue.Brokers;

import com.distributedQueue.Adapters.MessageAdapter;
import com.distributedQueue.Core.Message;
import com.distributedQueue.Core.Partition;
import com.distributedQueue.Core.Topic;
import com.distributedQueue.Logging.LogRepository;
import com.distributedQueue.Logging.LogMessage;
import com.distributedQueue.Metadata.PartitionMetadata;

import com.distributedQueue.Protocols.GossipProtocol;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.*;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Broker {

    public int getBrokerId() {
        return brokerId;
    }

    private final int brokerId;
    private final String host;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    private final int port;
    private final String controllerHost;
    private final int controllerPort;
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();
    private final Map<String, Map<Integer, PartitionMetadata>> metadataCache = new ConcurrentHashMap<>();
    private boolean isRunning = true;
    private GossipProtocol gossipProtocol;
    private final List<Broker> allBrokers;

    public Broker(int brokerId, String host, int port, String controllerHost, int controllerPort,
                  List<Broker> allBrokers) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.controllerHost = controllerHost;
        this.controllerPort = controllerPort;
        this.allBrokers = allBrokers; // Store the list of all brokers
    }

    public void start() throws IOException {
        // Initialize and start gossip protocol with the list of all brokers
        gossipProtocol = new GossipProtocol(allBrokers);
        Thread gossipThread = new Thread(() -> gossipProtocol.startGossip());
        gossipThread.start(); // Start gossip in a separate thread

        // Start HTTP server to accept requests
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/replicateMessage", new ReplicateMessageHandler());
        server.createContext("/updateLeadership", new UpdateLeadershipHandler());
        server.createContext("/publishMessage", new PublishMessageHandler());
        server.createContext("/health", new HealthCheckHandler());
        server.createContext("/sendGossip", new SendGossipHandler());
        server.createContext("/receiveGossip", new ReceiveGossipHandler());
        server.createContext("/brokers/leader", new LeaderBrokerHandler());
        server.createContext("/longPolling", new LongPollingHandler());
        server.createContext("/logs/broker/stream", new BrokerLogsStreamHandler());

        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        // Register with controller
        registerWithController();

        // Start sending heartbeats
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                this::sendHeartbeat, 0, 2000, TimeUnit.MILLISECONDS);

        // Add a shutdown hook to stop the gossip protocol and other resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down broker...");
            try {
                gossipProtocol.stopGossip(); // Stop the gossip protocol
                gossipThread.join(); // Wait for the gossip thread to terminate
                server.stop(0); // Stop the HTTP server
                System.out.println("Broker shut down gracefully.");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Shutdown interrupted: " + e.getMessage());
            }
        }));

        System.out.println("Broker started and running on port " + port);
    }

    public void addTopic(Topic topic) {
        topics.put(topic.getName(), topic);
        System.out.println("Broker " + brokerId + " added topic: " + topic.getName());
    }

    private void registerWithController() {
        try {
            URL url = new URL("http://" + controllerHost + ":" + controllerPort + "/registerBroker");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);

            String params = "brokerId=" + brokerId + "&host=" + host + "&port=" + port;
            OutputStream os = conn.getOutputStream();
            os.write(params.getBytes());
            os.flush();
            os.close();

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Broker " + brokerId + " registered with controller.");
            } else {
                System.err.println("Failed to register broker " + brokerId + " with controller.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendHeartbeat() {
        if (isRunning) {
            try {
                URL url = new URL(
                        "http://" + controllerHost + ":" + controllerPort + "/heartbeat?brokerId=" + brokerId);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    System.out.println("Broker " + brokerId + " sent heartbeat.");
                } else {
                    System.err.println("Failed to send heartbeat from broker " + brokerId);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Handlers for HTTP requests
    class ReplicateMessageHandler implements HttpHandler {

        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageAdapter())
                .create();

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
                return;
            }
            long startTime = System.currentTimeMillis();

            try (InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
                 OutputStream os = exchange.getResponseBody()) {

                // Deserialize message from the request body using Gson
                Message message = gson.fromJson(reader, Message.class);

                // Proceed with replication
                replicateMessage(message);

                String response = "Message replicated";
                exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length);
                os.write(response.getBytes(StandardCharsets.UTF_8));
            } catch (JsonSyntaxException e) {
                // Handle invalid JSON format
                e.printStackTrace();
                String errorMessage = "Invalid JSON format: " + e.getMessage();
                exchange.sendResponseHeaders(400, errorMessage.getBytes(StandardCharsets.UTF_8).length);
                exchange.getResponseBody().write(errorMessage.getBytes(StandardCharsets.UTF_8)); // Send error message in response
            } catch (Exception e) {
                // Handle other exceptions
                e.printStackTrace();
                String errorMessage = "Internal server error: " + e.getMessage();
                exchange.sendResponseHeaders(500, errorMessage.getBytes(StandardCharsets.UTF_8).length);
                exchange.getResponseBody().write(errorMessage.getBytes(StandardCharsets.UTF_8)); // Send error message in response
            }
            finally {
                long endTime = System.currentTimeMillis();
                String logMessage = "Message replication took " + (endTime - startTime) + " ms";
                LogRepository.addLog("Broker", logMessage);
            }
        }
    }

    class LongPollingHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
                return;
            }

            String query = exchange.getRequestURI().getQuery();
            if (query == null || query.isEmpty()) {
                exchange.sendResponseHeaders(400, -1); // Bad Request
                return;
            }

            // Parse query parameters to get topic, partition, and offset
            Map<String, String> queryParams;
            try {
                queryParams = parseQueryParameters(query);
            } catch (Exception e) {
                sendErrorResponse(exchange, 400, "Invalid query parameters");
                return;
            }

            String topicName = queryParams.get("topicName");
            int partitionId;
            long offset;

            try {
                partitionId = Integer.parseInt(queryParams.get("partitionId"));
                offset = Long.parseLong(queryParams.get("offset"));
            } catch (NumberFormatException e) {
                sendErrorResponse(exchange, 400, "Invalid partitionId or offset");
                return;
            }

            // Get the topic and partition
            Topic topic = topics.get(topicName);
            if (topic == null) {
                sendErrorResponse(exchange, 404, "Topic not found");
                return;
            }

            Partition partition = topic.getPartition(partitionId);
            if (partition == null) {
                sendErrorResponse(exchange, 404, "Partition not found");
                return;
            }

            long startTime = System.currentTimeMillis();
            long timeout = 500000; // 5 minutes
            int pollingInterval = 1000; // polling interval (1 second)

            // Long polling: wait until new messages are available in the partition
            while (partition.getCurrentOffset() <= offset) {
                if (System.currentTimeMillis() - startTime > timeout) {
                    sendErrorResponse(exchange, 408, "Timeout reached, no new messages available.");
                    return;
                }

                try {
                    Thread.sleep(pollingInterval);
                    pollingInterval = Math.min(pollingInterval * 2, 5000); // Exponential backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    sendErrorResponse(exchange, 500, "Internal Server Error");
                    return;
                }
            }

            // Fetch and return new messages
            List<Message> messages;
            try {
                messages = partition.getMessages(offset);
                if (messages == null || messages.isEmpty()) {
                    messages = new ArrayList<>(); // Return an empty list if no new messages
                }
            } catch (Exception e) {
                sendErrorResponse(exchange, 500, "Error retrieving messages");
                return;
            }

            Gson gson = new GsonBuilder()
                    .registerTypeAdapter(Message.class, new MessageAdapter())
                    .create();
            String response;
            try {
                response = gson.toJson(messages);
            } catch (Exception e) {
                sendErrorResponse(exchange, 500, "Error serializing messages");
                return;
            }

            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
                os.flush(); // Ensure the response is sent properly
            }
        }

        private void sendErrorResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            String jsonResponse = "{\"status\":\"error\", \"message\":\"" + message + "\"}";
            exchange.sendResponseHeaders(statusCode, jsonResponse.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(jsonResponse.getBytes());
            }
        }

        private Map<String, String> parseQueryParameters(String query) {
            return Arrays.stream(query.split("&"))
                    .map(param -> param.split("="))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
        }
    }

    class UpdateLeadershipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                // Method not allowed
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            // Use try-with-resources for safe stream handling
            try (ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
                 OutputStream os = exchange.getResponseBody()) {

                // Deserialize topic name and metadata
                String topicName = (String) in.readObject();
                PartitionMetadata partitionMetadata = (PartitionMetadata) in.readObject();

                // Perform leadership update
                updateLeadership(topicName, partitionMetadata);

                // Send response
                String response = "Leadership updated";
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length());
                os.write(response.getBytes());
            } catch (ClassNotFoundException e) {
                // Handle deserialization errors
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Internal server error: Invalid object format.");
            } catch (IOException e) {
                // Handle IO exceptions
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Internal server error: Unable to process the request.");
            } catch (Exception e) {
                // Handle unexpected exceptions
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "An unexpected error occurred.");
            }
        }

        private void sendErrorResponse(HttpExchange exchange, int statusCode, String errorMessage) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(statusCode, errorMessage.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(errorMessage.getBytes());
            }
        }
    }

    class PublishMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            try (ObjectInputStream in = new ObjectInputStream(exchange.getRequestBody());
                 OutputStream os = exchange.getResponseBody()) {

                // Deserialize message
                Message message = (Message) in.readObject();

                // Publish the message
                publishMessage(message);

                // Respond with success
                String response = "Message published";
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.length());
                os.write(response.getBytes());
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Invalid message format.");
            } catch (IOException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Error processing the request.");
            } catch (Exception e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "An unexpected error occurred.");
            }
        }

        private void sendErrorResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(statusCode, message.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(message.getBytes());
            }
        }
    }

    class ConsumeMessagesHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            try {
                String query = exchange.getRequestURI().getQuery();
                if (query == null || query.isEmpty()) {
                    throw new IllegalArgumentException("Missing query parameters.");
                }

                // Parse query parameters
                Map<String, String> queryParams = parseQueryParameters(query);
                String topicName = queryParams.get("topicName");
                int partitionId = Integer.parseInt(queryParams.get("partitionId"));
                int offset = Integer.parseInt(queryParams.get("offset"));

                if (topicName == null) {
                    throw new IllegalArgumentException("Missing 'topicName' parameter.");
                }

                // Fetch messages
                List<Message> messages = getMessagesForPartition(topicName, partitionId, offset);

                // Serialize messages to JSON using Gson
                Gson gson = new Gson();
                String response = gson.toJson(messages);

                // Respond with serialized messages in JSON
                exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
                exchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 400, "Invalid query parameters: " + e.getMessage());
            } catch (IOException e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "Error processing the request.");
            } catch (Exception e) {
                e.printStackTrace();
                sendErrorResponse(exchange, 500, "An unexpected error occurred.");
            }
        }

        // Example method to send error responses
        private void sendErrorResponse(HttpExchange exchange, int statusCode, String message) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
            String errorResponse = "{\"error\": \"" + message + "\"}";
            exchange.sendResponseHeaders(statusCode, errorResponse.getBytes().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(errorResponse.getBytes());
            }
        }

        private Map<String, String> parseQueryParameters(String query) {
            return Arrays.stream(query.split("&"))
                    .map(param -> param.split("="))
                    .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
        }
    }

    class HealthCheckHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); // 405 Method Not Allowed
                return;
            }

            String response = "OK";
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");
            long startTime = System.currentTimeMillis();

            try {
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } catch (Exception e) {
                e.printStackTrace();
                String errorResponse = "Internal Server Error";
                exchange.sendResponseHeaders(500, errorResponse.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorResponse.getBytes());
                }

                finally {
                    long endTime = System.currentTimeMillis();

                    String logMessage = "Health check request processing took " + (endTime - startTime) + " ms";
                    LogRepository.addLog("Broker", logMessage);
                }
            }
        }
    }

    // Handler for /sendGossip endpoint
    private class SendGossipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");

                try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                     ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                     OutputStream outputStream = exchange.getResponseBody()) {

                    // Serialize local state (e.g., partition metadata, leadership info)
                    objectOutputStream.writeObject(metadataCache);
                    objectOutputStream.flush();

                    byte[] response = byteArrayOutputStream.toByteArray();
                    exchange.sendResponseHeaders(200, response.length);
                    outputStream.write(response);
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }
    }

    // Handler for /receiveGossip endpoint
    private class ReceiveGossipHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "text/plain");

                try (InputStream inputStream = exchange.getRequestBody();
                     ObjectInputStream objectInputStream = new ObjectInputStream(inputStream)) {

                    @SuppressWarnings("unchecked")
                    Map<String, Map<Integer, PartitionMetadata>> receivedState = (Map<String, Map<Integer, PartitionMetadata>>) objectInputStream
                            .readObject();

                    gossipProtocol.reconcileState(receivedState, Broker.this);
                    exchange.sendResponseHeaders(200, 0); // OK
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    String errorMessage = "Failed to deserialize received state.";
                    exchange.sendResponseHeaders(500, errorMessage.length());
                    try (OutputStream outputStream = exchange.getResponseBody()) {
                        outputStream.write(errorMessage.getBytes());
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

    // Handler for /brokers/leader endpoint
    private class LeaderBrokerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=UTF-8");

                try {
                    Map<String, String> params = queryToMap(exchange.getRequestURI().getQuery());
                    String topic = params.get("topic");

                    if (topic == null || topic.isEmpty()) {
                        String errorMessage = "{\"error\": \"Missing 'topic' query parameter\"}";
                        exchange.sendResponseHeaders(400, errorMessage.getBytes().length); // Bad Request
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(errorMessage.getBytes());
                        }
                        return;
                    }

                    PartitionMetadata leaderMetadata = getLeaderForTopic(topic);

                    if (leaderMetadata != null) {
                        String response = new Gson().toJson(leaderMetadata);
                        exchange.sendResponseHeaders(200, response.getBytes().length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(response.getBytes());
                        }
                    } else {
                        String errorMessage = "{\"error\": \"Leader not found for the specified topic\"}";
                        exchange.sendResponseHeaders(404, errorMessage.getBytes().length); // Not Found
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(errorMessage.getBytes());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    exchange.sendResponseHeaders(500, -1); // Internal Server Error
                }
            } else {
                exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            }
        }

        private PartitionMetadata getLeaderForTopic(String topic) {
            Map<Integer, PartitionMetadata> partitions = metadataCache.get(topic);
            if (partitions != null) {
                for (PartitionMetadata metadata : partitions.values()) {
                    if (metadata.getLeaderId() == brokerId) {
                        return metadata;
                    }
                }
            }
            return null;
        }

        private Map<String, String> queryToMap(String query) {
            Map<String, String> result = new HashMap<>();
            if (query != null && !query.isEmpty()) {
                for (String param : query.split("&")) {
                    String[] entry = param.split("=");
                    if (entry.length > 1) {
                        result.put(entry[0], entry[1]);
                    } else {
                        result.put(entry[0], "");
                    }
                }
            }
            return result;
        }
    }

    public void publishMessage(Message message) {
        Topic topic = topics.get(message.getTopic());

        if (topic == null) {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(message.getTopic());
            topic = topics.get(message.getTopic());
            if (topic == null) {
                System.err.println("Topic not found after metadata fetch: " + message.getTopic());
                return;
            }
        }

        Partition partition = topic.getPartition(message.getPartition());

        // Check if this broker is the leader for the partition
        PartitionMetadata partitionMetadata = topic.getPartitionMetadata(message.getPartition());
        if (partitionMetadata.getLeaderId() != brokerId) {
            System.err.println("Broker " + brokerId + " is not the leader for partition " + message.getPartition());
            return;
        }

        partition.addMessage(message); // Handles message publishing and replication internally

        // Replicate message to followers
        replicateMessageToFollowers(message, partitionMetadata.getFollowers());
    }

    /*
     * private void replicateMessageToFollowers(Message message, List<Integer>
     * followerIds) {
     * for (int followerId : followerIds) {
     * BrokerInfo followerInfo = fetchBrokerInfo(followerId);
     * if (followerInfo != null) {
     * try {
     * URL url = new URL(
     * "http://" + followerInfo.getHost() + ":" + followerInfo.getPort()
     * + "/replicateMessage");
     * HttpURLConnection conn = (HttpURLConnection) url.openConnection();
     * conn.setRequestMethod("POST");
     * conn.setDoOutput(true);
     *
     * // Serialize the message object
     * ObjectOutputStream out = new ObjectOutputStream(conn.getOutputStream());
     * out.writeObject(message);
     * out.flush();
     * out.close();
     *
     * int responseCode = conn.getResponseCode();
     * if (responseCode == 200) {
     * System.out
     * .println("Replicated message " + message.getMessageId() + " to follower "
     * + followerId);
     * } else {
     * System.err.println("Failed to replicate message to follower " + followerId);
     * }
     * } catch (IOException e) {
     * e.printStackTrace();
     * }
     * } else {
     * System.err.println("Follower broker " + followerId + " info not available.");
     * }
     * }
     * }
     */

    private void replicateMessageToFollowers(Message message, List<Integer> followerIds) {
        if (!waitForReadiness()) {
            System.err.println("Replication aborted: Controller or brokers not ready.");
            return;
        }

        // Create Gson instance with MessageAdapter
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(Message.class, new MessageAdapter())  // Register your custom MessageAdapter
                .create();

        // Serialize the message to JSON
        String messageJson = gson.toJson(message);

        for (int followerId : followerIds) {

            if (followerId == brokerId) {
                // System.out.println("Skipping self-replication for Broker " + brokerId);
                continue;
            }

            BrokerInfo followerInfo = fetchBrokerInfo(followerId);

            // Check metadata availability
            if (followerInfo == null) {
                //System.err.println("Follower broker " + followerId + " metadata not available.");
                continue;
            }

            // Proceed with replication
            try {
                String urlStr = "http://" + followerInfo.getHost() + ":" + followerInfo.getPort() + "/replicateMessage";

                URL url = new URL(urlStr);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setRequestProperty("Content-Type", "application/json"); // Set content type to JSON

                // Write the serialized JSON message to the output stream
                try (OutputStream os = conn.getOutputStream()) {
                    byte[] input = messageJson.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }

                // Get and log response code
                int responseCode = conn.getResponseCode();
                if (responseCode == 200) {
                    // System.out.println("Replicated message " + message.getOffset() + " to follower " + followerId);
                } else {
                    System.err.println("Failed to replicate message to follower " + followerId + ". Response Code: "
                            + responseCode);
                }
            } catch (IOException e) {
                System.err.println("Error replicating message to follower " + followerId);
                e.printStackTrace();
            }
        }
    }


    private boolean waitForReadiness() {
        int maxRetries = 10; // Maximum number of retries
        int delay = 2000; // Delay between retries (milliseconds)

        for (int i = 0; i < maxRetries; i++) {
            if (isControllerReady()) {
                return true;
            }
            //System.out.println("Controller not ready. Retrying in " + (delay / 1000) + " seconds...");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Readiness check interrupted.");
                return false;
            }
        }
        System.err.println("Controller did not become ready after multiple attempts.");
        return false;
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
                // System.out.println("Controller is ready.");
                return true;
            } else if (responseCode == 503) {
                // Controller is not ready
                // System.out.println("Controller is not ready.");
            } else {
                System.err.println("Unexpected response code from readiness check: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error checking controller readiness: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    /*public void replicateMessage(Message message) {

        Topic topic = topics.get(message.getTopic());

        if (topic == null) {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(message.getTopic());
            topic = topics.get(message.getTopic());
            if (topic == null) {
                System.err.println("Topic not found after metadata fetch: " + message.getTopic());
                return;
            }
        }

        // Iterate over the partitions in the topic (HashMap)
        for (Map.Entry<Integer, Partition> entry : topic.getPartitions().entrySet()) {
            Integer partitionId = entry.getKey();
            Partition partition = entry.getValue();

            // Retrieve the PartitionMetadata
            PartitionMetadata partitionMetadata = partition.getPartitionMetadata();

            // Get the follower IDs from the PartitionMetadata
            List<Integer> followerIds = partitionMetadata.getFollowerIds();

            // Append the message to the partition
            partition.addMessage(message);
            System.out.println("Broker " + brokerId + " replicated message " + message.getOffset() + " to partition "
                    + partitionId);

            // Replicate the message to followers
            replicateMessageToFollowers(message, followerIds);
        }
    }*/

    public void replicateMessage(Message message) {
        // Fetch the metadata cache for the topic
        Map<Integer, PartitionMetadata> partitionMetadataMap = getMetadataCache().get(message.getTopic());

        if (partitionMetadataMap == null) {
            //System.err.println("Metadata not found for topic: " + message.getTopic());
            return;
        }

        // Retrieve the PartitionMetadata for the partitionId
        PartitionMetadata partitionMetadata = partitionMetadataMap.get(message.getPartition());
        if (partitionMetadata == null) {
            System.err.println("Partition metadata not found for partition: " + message.getPartition() + " in topic: " + message.getTopic());
            return;
        }

        // Get the follower IDs from the PartitionMetadata
        List<Integer> followerIds = partitionMetadata.getFollowerIds();

        // Replicate the message to the partition
        Topic topic = topics.get(message.getTopic());
        if (topic == null) {
            System.err.println("Topic not found: " + message.getTopic());
            return;
        }

        // Retrieve the partition object from the topic (this should exist based on the topic's partitions)
        Partition partition = topic.getPartition(message.getPartition());
        if (partition == null) {
            System.err.println("Partition not found for partitionId: " + message.getPartition() + " in topic: " + message.getTopic());
            return;
        }

        // Append the message to the partition
        partition.addMessage(message);
        System.out.println("Broker " + brokerId + " replicated message " + message.getOffset() + " to partition " + message.getPartition());

        // Replicate the message to followers
        replicateMessageToFollowers(message, followerIds);
    }


    public List<Message> getMessagesForPartition(String topicName, int partitionId, int offset) {
        Topic topic = topics.get(topicName);
        if (topic == null) {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(topicName);
            topic = topics.get(topicName);
            if (topic == null) {
                System.err.println("Topic not found after metadata fetch: " + topicName);
                return Collections.emptyList();
            }
        }

        Partition partition = topic.getPartition(partitionId);
        return partition.getMessages(offset);
    }

    public void updateLeadership(String topicName, PartitionMetadata partitionMetadata) {
        Topic topic = topics.get(topicName);
        if (topic != null) {
            topic.updatePartitionMetadata(partitionMetadata.getPartitionId(), partitionMetadata);

            Partition partition = topic.getPartition(partitionMetadata.getPartitionId());
            if (partition != null) {
                if (partitionMetadata.getLeaderId() == brokerId) {
                    partition.setLeader(true);
                    System.out
                            .println("Broker " + brokerId + " is now leader for topic " + topicName + " partition "
                                    + partitionMetadata.getPartitionId());
                } else {
                    partition.setLeader(false);
                }
            }
        } else {
            // Fetch metadata and create topic locally if not present
            fetchMetadataForTopic(topicName);
            // Recursive call to update leadership now that topic exists
            updateLeadership(topicName, partitionMetadata);
        }
    }

    private void fetchMetadataForTopic(String topicName) {
        try {
            // Create the URL for fetching metadata
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getMetadata?topicName=" + topicName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            // Check the response code
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Read the response
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    StringBuilder responseBuilder = new StringBuilder();
                    String line;
                    while ((line = in.readLine()) != null) {
                        responseBuilder.append(line);
                    }

                    // Parse the JSON response into a Map
                    String response = responseBuilder.toString();
                    Gson gson = new Gson();
                    Type type = new TypeToken<Map<Integer, PartitionMetadata>>() {
                    }.getType();
                    Map<Integer, PartitionMetadata> topicMetadata = gson.fromJson(response, type);

                    // Cache the metadata
                    metadataCache.put(topicName, topicMetadata);

                    // Create the topic and its partitions locally
                    Topic topic = new Topic(topicName, topicMetadata.size(), 1); // Replication factor not used here
                    for (PartitionMetadata pm : topicMetadata.values()) {
                        topic.updatePartitionMetadata(pm.getPartitionId(), pm);
                        Partition partition = topic.getPartition(pm.getPartitionId());
                        if (partition != null) {
                            partition.setLeader(pm.getLeaderId() == brokerId);
                        }
                    }
                    topics.put(topicName, topic);
                    System.out.println("Fetched metadata and created topic " + topicName);
                }
            } else {
                //System.err.println("Failed to fetch metadata for topic " + topicName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, Map<Integer, PartitionMetadata>> getMetadataCache() {
        return metadataCache;
    }

    private BrokerInfo fetchBrokerInfo(int brokerId) {

        try {
            // Construct the URL to fetch the broker information from the controller
            URL url = new URL(
                    "http://" + controllerHost + ":" + controllerPort + "/getBrokerInfo?brokerId=" + brokerId);

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            // Check the response code
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Read the response from the controller
                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                    String response = in.readLine(); // Expect response like "host:port" or "Broker not found"

                    if ("Broker not found".equals(response)) {
                        System.err.println("Broker " + brokerId + " not found.");
                        return null;
                    }

                    // Parse the host and port
                    String[] hostPort = response.split(":");
                    if (hostPort.length == 2) {
                        String host = hostPort[0];
                        int port = Integer.parseInt(hostPort[1]);
                        // Return a new BrokerInfo object
                        return new BrokerInfo(host, port);
                    }
                }
            } else {
                System.err.println("Failed to fetch broker info. Response Code: " + responseCode);
            }
        } catch (IOException e) {
            System.err.println("Error fetching broker metadata for broker " + brokerId);
            e.printStackTrace();
        }
        return null;

    }
    class BrokerLogsStreamHandler implements HttpHandler {
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
                List<LogMessage> brokerLogs = LogRepository.getLogsBySource("Broker");

                // Map LogMessages to a JSON structure
                List<String> logMessages = brokerLogs.stream()
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
                    Thread.sleep(5000); // Wait for 5 seconds before sending new data
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    // BrokerInfo class to store broker's network information
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

    // Stop the broker
    public void stop() {
        isRunning = false;
    }
}