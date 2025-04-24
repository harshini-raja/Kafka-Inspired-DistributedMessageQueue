import React, { useState, useEffect } from "react";
import '../css/Dashboard.css';


const Dashboard = () => {
  const [brokers, setBrokers] = useState([]);
  const [activeBrokers, setActiveBrokers] = useState([]);
  const [topicLeaders, setTopicLeaders] = useState({});
  const [controllerLogs, setControllerLogs] = useState([]);
  const [brokerLogs, setBrokerLogs] = useState([]);
  const [producerLogs, setProducerLogs] = useState([]);
  const [consumerLogs, setConsumerLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState("welcome"); // Track the active tab
  const topics = ["TopicA", "TopicB", "TopicC"];

  const brokerList = [
    { id: 1, name: "Broker 1", url: "http://localhost:8085/health" },
    { id: 2, name: "Broker 2", url: "http://localhost:8086/health" },
    { id: 3, name: "Broker 3", url: "http://localhost:8087/health" },
  ];

  // For polling
  useEffect(() => {
    const intervalId = setInterval(() => {
      fetchLogs("http://localhost:8082/logs", setProducerLogs);
      fetchLogs("http://localhost:8083/logs", setConsumerLogs);
    }, 5000); // Fetch every 5 seconds

    return () => clearInterval(intervalId);
  }, []);

  // Fetch leader for a specific topic
  const fetchLeaderForTopic = async (topic) => {
    try {
      const response = await fetch(
        `http://localhost:8085/brokers/leader?topic=${topic}`
      );
      if (response.ok) {
        const leaderData = await response.json();
        return leaderData.leader; // Assuming `leader` contains broker details
      } else {
        console.error(
          `Failed to fetch leader for topic ${topic}: ${response.statusText}`
        );
        return null; // Return null on failure
      }
    } catch (error) {
      console.error(`Error fetching leader for topic ${topic}:`, error);
      return null; // Return null on exception
    }
  };

  // Fetch leaders for all topics
  const fetchLeadersForTopics = async () => {
    const leaderPromises = topics.map((topic) => fetchLeaderForTopic(topic));
    const results = await Promise.all(leaderPromises);

    const leadersMap = {};
    topics.forEach((topic, index) => {
      leadersMap[topic] = results[index];
    });

    setTopicLeaders(leadersMap); // Set leaders for all topics
  };

  // Fetch active brokers
  const fetchActiveBrokers = async () => {
    try {
      const response = await fetch("http://localhost:8090/brokers/active"); // Update this with correct endpoint
      if (response.ok) {
        const activeBrokersData = await response.json();
        setActiveBrokers(activeBrokersData);
      }
    } catch (error) {
      console.error("Error fetching active brokers:", error);
    }
  };

  const fetchLogs = async (endpoint, setState) => {
    try {
      const response = await fetch(endpoint);
      if (response.ok) {
        const data = await response.json();
        setState(data);
      } else {
        console.error(
          `Error fetching logs from ${endpoint}: ${response.statusText}`
        );
      }
    } catch (error) {
      console.error(`Error fetching logs from ${endpoint}:`, error);
    }
  };

  const fetchAllLogs = () => {
    fetchLogs(
      "http://localhost:8090/logs/controller/stream",
      setControllerLogs
    );
    fetchLogs("http://localhost:8090/logs/broker/stream", setBrokerLogs);
    fetchLogs("http://localhost:8082/logs", setProducerLogs);
    fetchLogs("http://localhost:8083/logs", setConsumerLogs);
  };

  const fetchBrokers = async () => {
    try {
      setLoading(true);
      const brokerPromises = brokerList.map(async (broker) => {
        try {
          const response = await fetch(broker.url);
          if (response.ok) {
            return { ...broker, status: "Healthy" };
          }
          return { ...broker, status: "Unhealthy" };
        } catch (error) {
          return { ...broker, status: "Unreachable" };
        }
      });
      const brokerStatuses = await Promise.all(brokerPromises);
      setBrokers(brokerStatuses);
    } catch (err) {
      setError("Failed to fetch brokers data");
    } finally {
      setLoading(false);
    }
  };

  // Set up the EventSource listener for real-time logs
  useEffect(() => {
    const eventSourceController = new EventSource(
      "http://localhost:8090/logs/controller/stream"
    );
    const eventSourceBroker = new EventSource(
      "http://localhost:8090/logs/broker/stream"
    );
    const eventSourceProducer = new EventSource(
      "http://localhost:8090/logs/producer/stream"
    );
    const eventSourceConsumer = new EventSource(
      "http://localhost:8090/logs/consumer/stream"
    );

    eventSourceController.onmessage = (event) => {
      const newLog = JSON.parse(event.data);
      setControllerLogs((prevLogs) => [...prevLogs, newLog]); // Append new log to the state
    };

    eventSourceBroker.onmessage = (event) => {
      const newLog = JSON.parse(event.data);
      setBrokerLogs((prevLogs) => [...prevLogs, newLog]);
    };

    eventSourceProducer.onmessage = (event) => {
      const newLog = JSON.parse(event.data);
      setProducerLogs((prevLogs) => [...prevLogs, newLog]);
    };

    eventSourceConsumer.onmessage = (event) => {
      const newLog = JSON.parse(event.data);
      setConsumerLogs((prevLogs) => [...prevLogs, newLog]);
    };

    return () => {
      eventSourceController.close();
      eventSourceBroker.close();
      eventSourceProducer.close();
      eventSourceConsumer.close();
    };
  }, []);

  useEffect(() => {
    fetchLeadersForTopics();
    fetchBrokers();
    fetchActiveBrokers();
    fetchAllLogs();
  }, []);

  // Render content for each tab
  const renderTabContent = () => {
    if (activeTab === "welcome") {
      return <p>This is a Distributed Queue Architecture Inspired by Kafka</p>;
    }
    if (activeTab === "brokerStatus") {
      return (
        <div>
          <h2>Brokers Health</h2>
          <ul>
            {brokers.map((broker) => (
              <li key={broker.id}>
                {broker.name} -
                {broker.status === "Healthy"
                  ? "✅ Healthy"
                  : broker.status === "Unhealthy"
                  ? "❌ Unhealthy"
                  : "⚠️ Unreachable"}
                {Object.entries(topicLeaders).map(([topic, leader]) =>
                  leader === broker.name ? (
                    <span key={topic}> | Leads {topic}</span>
                  ) : null
                )}
              </li>
            ))}
          </ul>

          {/* Active Brokers Section */}
          <h2>Active Brokers</h2>
          <ul>
            {activeBrokers.length > 0 ? (
              activeBrokers.map((broker) => (
                <li key={broker.brokerId}>
                  {broker.host}:{broker.port} (Broker ID: {broker.brokerId})
                </li>
              ))
            ) : (
              <p>No active brokers found.</p>
            )}
          </ul>
        </div>
      );
    }
    if (activeTab === "producerConsumerStats") {
      return (
        <div>
          <h2>Producer Logs</h2>
          <ul>
            {producerLogs.map((log, index) => (
              <li key={index}>{log}</li>
            ))}
          </ul>
          <h2>Consumer Logs</h2>
          <ul>
            {consumerLogs.map((log, index) => (
              <li key={index}>{log}</li>
            ))}
          </ul>
        </div>
      );
    }
    if (activeTab === "controllerStatus") {
      return (
        <div>
          <h2>Controller Logs</h2>
          <ul>
            {controllerLogs.map((log, index) => (
              <li key={index}>{log.message}</li>
            ))}
          </ul>
        </div>
      );
    }
  };

  return (
    <div>
      <h1>Distributed Queue System Dashboard</h1>
      <div className="tabs">
        <button onClick={() => setActiveTab("welcome")}>Welcome</button>
        <button onClick={() => setActiveTab("brokerStatus")}>
          Broker Status
        </button>
        <button onClick={() => setActiveTab("producerConsumerStats")}>
          Producer & Consumer Stats
        </button>
        <button onClick={() => setActiveTab("controllerStatus")}>
          Controller Status
        </button>
      </div>
      <div className="tab-content">{renderTabContent()}</div>
    </div>
  );
};

export default Dashboard;
