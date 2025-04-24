import React, { useEffect, useState } from 'react';
import { fetchConsumerLogs } from '../services/Api';


const ConsumerLogs = () => {
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    const interval = setInterval(async () => {
      const data = await fetchConsumerLogs();
      setLogs(data);
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="tab-content">
      <h2>Consumer Logs</h2>
      <ul>
        {logs.map((log, idx) => (
          <li key={idx}>[{new Date().toLocaleTimeString()}] {log}</li>
        ))}
      </ul>

    </div>
  );
};

export default ConsumerLogs;
