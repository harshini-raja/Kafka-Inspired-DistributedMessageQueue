package com.distributedQueue.Logging;

public class LogMessage {
    private String source;   // e.g., "Broker", "Controller"
    private String message;
    private String timestamp; // Optional for better tracking

    public LogMessage(String source, String message, String timestamp) {
        this.source = source;
        this.message = message;
        this.timestamp = timestamp;
    }

    public String getSource() {
        return source;
    }

    public String getMessage() {
        return message;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
