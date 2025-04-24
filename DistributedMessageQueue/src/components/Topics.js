import React, { useEffect, useState } from "react";
import { getTopics } from "../services/Api";

const Topics = () => {
  const [topics, setTopics] = useState([]);

  useEffect(() => {
    getTopics().then((response) => {
      setTopics(response.data);
    });
  }, []);

  return (
    <div>
      <h1>Topics</h1>
      <ul>
        {topics.map((topic) => (
          <li key={topic.name}>{topic.name}</li>
        ))}
      </ul>
    </div>
  );
};

export default Topics;
