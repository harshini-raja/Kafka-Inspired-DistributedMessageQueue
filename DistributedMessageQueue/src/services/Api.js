import axios from 'axios';

const API_BASE_URL = 'http://localhost:8090'; // Change this to your backend URL

export const getBrokers = () => {
  return axios.get(`${API_BASE_URL}/brokers`);
};

export const getTopics = () => {
  return axios.get(`${API_BASE_URL}/topics`);
};

export const getMessages = (topic, partitionId, offset) => {
  return axios.get(`${API_BASE_URL}/messages`, {
    params: { topic, partitionId, offset }
  });
};