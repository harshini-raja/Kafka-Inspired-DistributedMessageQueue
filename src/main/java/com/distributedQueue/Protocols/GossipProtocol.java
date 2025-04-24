package com.distributedQueue.Protocols;

import com.distributedQueue.Brokers.Broker;
import com.distributedQueue.Metadata.PartitionMetadata;

import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GossipProtocol {

    private final List<Broker> peers;  // List of all brokers (peers) in the gossip network
    private final Random random = new Random();
    private boolean running = true;  // Control flag for the gossip loop

    public GossipProtocol(List<Broker> peers) {
        this.peers = peers;  // Accept a list of brokers as peers
    }

    // Start the gossip process
    public void startGossip() {
        while (running) {
            try {
                Broker peer = getRandomPeer();
                gossipPush(peer);  // Push state to a random peer
                gossipPull();  // Pull state from a random peer
                Thread.sleep(5000); // Sleep for 5 seconds before the next gossip round
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupt status
                break;
            } catch (Exception e) {
                System.err.println("Error during gossip exchange: " + e.getMessage());
            }
        }
        System.out.println("Gossip protocol stopped.");
    }

    // Push local state to a random peer
    public void gossipPush(Broker peer) {
        try {
            // Serialize local state (e.g., partition metadata, leadership info)
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            // Assuming local state is a map of partition metadata
            Map<String, Map<Integer, PartitionMetadata>> localState = peer.getMetadataCache();
            objectOutputStream.writeObject(localState);
            objectOutputStream.flush();
            byte[] serializedState = byteArrayOutputStream.toByteArray();

            // Send the serialized state to the selected peer
            URL url = new URL("http://" + peer.getHost() + ":" + peer.getPort() + "/receiveGossip");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/octet-stream");

            try (OutputStream outputStream = conn.getOutputStream()) {
                outputStream.write(serializedState);
                outputStream.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Gossip push successful to peer " + peer.getBrokerId());
            } else {
                System.err.println("Failed to push gossip to peer " + peer.getBrokerId() + ", response code: " + responseCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Helper function to randomly pick a peer
    private Broker getRandomPeer() {
        int index = random.nextInt(peers.size());
        return peers.get(index);
    }

    // Pull state from a random peer
    public void gossipPull() {
        try {
            // Select a random peer
            Broker peer = getRandomPeer();

            // Request state from the peer
            URL url = new URL("http://" + peer.getHost() + ":" + peer.getPort() + "/sendGossip");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                try (ObjectInputStream ois = new ObjectInputStream(conn.getInputStream())) {
                    @SuppressWarnings("unchecked")
                    Map<String, Map<Integer, PartitionMetadata>> receivedState = (Map<String, Map<Integer, PartitionMetadata>>) ois.readObject();

                    // Reconcile received state with local state
                    reconcileState(receivedState, peer);
                }
            } else {
                System.err.println("Failed to pull gossip from peer " + peer.getBrokerId() + ", response code: " + responseCode);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void reconcileState(Map<String, Map<Integer, PartitionMetadata>> receivedState, Broker peer){
        for (Map.Entry<String, Map<Integer, PartitionMetadata>> entry : receivedState.entrySet()) {
            String topic = entry.getKey();
            Map<Integer, PartitionMetadata> receivedPartitions = entry.getValue();

            // Update local state only if it's stale
            Map<Integer, PartitionMetadata> localPartitions = peer.getMetadataCache().get(topic);
            if (localPartitions == null) {
                peer.getMetadataCache().put(topic, receivedPartitions);
            } else {
                for (Map.Entry<Integer, PartitionMetadata> partitionEntry : receivedPartitions.entrySet()) {
                    Integer partitionId = partitionEntry.getKey();
                    PartitionMetadata receivedMetadata = partitionEntry.getValue();
                    PartitionMetadata localMetadata = localPartitions.get(partitionId);

                    if (localMetadata == null || localMetadata.getLeaderId() != receivedMetadata.getLeaderId()) {
                        localPartitions.put(partitionId, receivedMetadata);
                    }
                }
            }
        }
    }

    // Stop the gossip process
    public void stopGossip() {
        running = false; // Signal the loop to exit
    }
}
