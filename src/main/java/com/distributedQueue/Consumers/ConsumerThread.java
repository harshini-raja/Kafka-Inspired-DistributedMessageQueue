package com.distributedQueue.Consumers;

public class ConsumerThread implements Runnable {
    private int consumerId;
    private String topic;
    private Consumer consumer;

    public ConsumerThread(int consumerId, String topic, Consumer consumer) {
        this.consumerId = consumerId;
        this.topic = topic;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        // Consume messages from the assigned topic
        System.out.println("Consumer " + consumerId + " is fetching messages from " + topic);
        consumer.consume(topic);
    }
}
