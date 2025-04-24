import React, { useEffect, useState } from "react";
import { getMessages } from "../services/Api";

const Messages = () => {
  const [messages, setMessages] = useState([]);
  const [topic, setTopic] = useState("TestTopic");
  const [partitionId, setPartitionId] = useState(0);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    getMessages(topic, partitionId, offset).then((response) => {
      setMessages(response.data);
    });
  }, [topic, partitionId, offset]);

  return (
    <div>
      <h1>Messages</h1>
      <ul>
        {messages.map((message, index) => (
          <li key={index}>{message}</li>
        ))}
      </ul>
    </div>
  );
};

export default Messages;
