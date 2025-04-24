import React, { useEffect, useState } from "react";
import { getBrokers } from "../services/Api";

const Brokers = () => {
  const [brokers, setBrokers] = useState([]);

  useEffect(() => {
    getBrokers().then((response) => {
      setBrokers(response.data);
    });
  }, []);

  return (
    <div>
      <h1>Brokers</h1>
      <ul>
        {brokers.map((broker) => (
          <li key={broker.id}>
            {broker.host}:{broker.port}
          </li>
        ))}
      </ul>
    </div>
  );
};

export default Brokers;
