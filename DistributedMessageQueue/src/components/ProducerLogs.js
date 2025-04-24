import React, { useEffect, useState } from 'react';
import { fetchProducerLogs } from '../services/Api';

const ProducerLogs = () => {
  const [logs, setLogs] = useState([]);

  useEffect(() => {
    const interval = setInterval(async () => {
      const data = await fetchProducerLogs();
      setLogs(data);
    }, 3000);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="tab-content">
      <h2>Producer Logs</h2>
      <ul>
        {logs.map((log, idx) => (
          <li key={idx}>[{new Date().toLocaleTimeString()}] {log}</li>
        ))}
      </ul>
    </div>
  );
};

export default ProducerLogs;
