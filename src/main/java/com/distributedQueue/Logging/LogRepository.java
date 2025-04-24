package com.distributedQueue.Logging;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import com.google.gson.Gson;

public class LogRepository {

    private static final Queue<LogMessage> logs = new ConcurrentLinkedQueue<>();

    public static void addLog(String source, String message) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        logs.add(new LogMessage(source, message, timestamp));
    }

    public static List<LogMessage> getLogsBySource(String source) {
        return logs.stream()
                .filter(log -> log.getSource().equals(source))
                .collect(Collectors.toList());
    }

    public static String getAllLogs() {
        return new Gson().toJson(logs);
    }

}
