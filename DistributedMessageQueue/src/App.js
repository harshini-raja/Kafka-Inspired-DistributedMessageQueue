import React from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Dashboard from "./components/Dashboard";
import Brokers from "./components/Broker";
import Topics from "./components/Topics";
import Messages from "./components/Message";

function App() {
  return (
    <Router>
      <div>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/brokers" element={<Brokers />} />
          <Route path="/topics" element={<Topics />} />
          <Route path="/messages" element={<Messages />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
