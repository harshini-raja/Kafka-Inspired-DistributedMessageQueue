package com.distributedQueue.Core;

import java.util.LinkedList;
import java.util.Queue;

public class MessageQueue {

    private Queue<Message> queue;

    public MessageQueue() {
        this.queue = new LinkedList<>();
    }

    public synchronized void addMessage(Message message) {
        queue.offer(message);
    }

    public synchronized Message getMessage() {
        return queue.poll();
    }

    public synchronized boolean isEmpty() {
        return queue.isEmpty();
    }

}
