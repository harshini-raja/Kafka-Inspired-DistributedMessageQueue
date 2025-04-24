package com.distributedQueue;

import com.distributedQueue.Brokers.Broker;
import com.distributedQueue.Controllers.Controller;
import com.distributedQueue.Producers.Producer;
import com.distributedQueue.Consumers.Consumer;
import com.distributedQueue.Consumers.ConsumerThread;

import java.util.ArrayList;
import java.util.List;

public class MainClass {

    public static void main(String[] args) throws Exception {
        String role = System.getenv("ROLE"); // "controller", "broker", "producer", "consumer"

        // Create a list of brokers to pass to each broker instance
        List<Broker> allBrokers = new ArrayList<>();
        String[] topicNames = { "TopicA", "TopicB", "TopicC" };

        switch (role.toLowerCase()) {
            case "controller":
                int controllerPort = Integer.parseInt(System.getenv("PORT"));
                Controller controller = new Controller(controllerPort);
                controller.start();
                System.out.println("Controller started on port " + controllerPort);
                break;

            case "broker":
                int brokerId = Integer.parseInt(System.getenv("BROKER_ID"));
                String host = System.getenv("HOST");
                int port = Integer.parseInt(System.getenv("PORT"));
                String controllerHost = System.getenv("CONTROLLER_HOST");
                int controllerPortBroker = Integer.parseInt(System.getenv("CONTROLLER_PORT"));

                // Create the broker and add it to the list
                Broker broker = new Broker(brokerId, host, port, controllerHost, controllerPortBroker, allBrokers);
                allBrokers.add(broker); // Add the newly created broker to the list

                // Start the broker
                broker.start();
                //System.out.println("Broker " + brokerId + " started on port " + port);
                break;

            case "producer":
                String controllerHostProducer = System.getenv("CONTROLLER_HOST");
                int controllerPortProducer = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
                Producer producer = new Producer(controllerHostProducer, controllerPortProducer);

                // Create three topics with 3 partitions and 2 replication factors each

                for (String topicName : topicNames) {
                    producer.createTopic(topicName, 3, 2); // 3 partitions, 2 replication factor
                    System.out.println("Created topic: " + topicName);
                    Thread.sleep(1000); // Wait for topic creation
                }

                // Send messages to each topic
                for (String topicName : topicNames) {
                    for (int i = 1; i <= 25; i++) {
                        String messageContent = "Message " + i + " for " + topicName;
                        producer.send(topicName, messageContent.getBytes());
                        System.out.println("Sent to " + topicName + ": " + messageContent);
                        Thread.sleep(500); // Simulate delay
                    }
                }

                Thread.sleep(1000); // Wait before exiting producer
                break;

            case "consumer":

                String controllerHostConsumer = System.getenv("CONTROLLER_HOST");
                int controllerPortConsumer = Integer.parseInt(System.getenv("CONTROLLER_PORT"));
                Consumer consumer = new Consumer(controllerHostConsumer, controllerPortConsumer);

                // Create and start consumer threads for each topic
                Thread consumer1Thread = new Thread(new ConsumerThread(1, topicNames[0], consumer)); // Consumer for TopicA
                Thread consumer2Thread = new Thread(new ConsumerThread(2, topicNames[1], consumer)); // Consumer for TopicB
                Thread consumer3Thread = new Thread(new ConsumerThread(3, topicNames[2], consumer)); // Consumer for TopicC

                consumer1Thread.start();
                consumer2Thread.start();
                consumer3Thread.start();

                // Optionally, wait for all threads to finish
                try {
                    consumer1Thread.join();
                    consumer2Thread.join();
                    consumer3Thread.join();

                    // After threads finish, you can interrupt them if needed (but only if they are still running)
                    if (consumer1Thread.isAlive()) {
                        consumer1Thread.interrupt();
                    }
                    if (consumer2Thread.isAlive()) {
                        consumer2Thread.interrupt();
                    }
                    if (consumer3Thread.isAlive()) {
                        consumer3Thread.interrupt();
                    }

                    // Log throughput and latency after all consumers finish
                    consumer.logThroughput();
                    consumer.logAverageLatency();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                System.out.println("All consumers have finished consuming.");

            default:
                System.err.println("Invalid ROLE specified. Use 'controller', 'broker', 'producer', or 'consumer'.");
                break;
        }
    }
}

