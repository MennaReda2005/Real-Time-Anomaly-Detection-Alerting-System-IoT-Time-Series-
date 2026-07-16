# Real-Time IoT Anomaly Detection & Alerting System

An end-to-end AI-powered system for real-time anomaly detection in IoT sensor data. The project continuously streams sensor readings, detects abnormal behavior using a deep learning model, stores time-series data, visualizes system status through Grafana dashboards, and instantly notifies users via Email and Telegram when anomalies are detected.

---

# Project Overview

This project simulates an industrial IoT environment where multiple sensors continuously generate real-time data. The data is streamed using Apache Kafka, analyzed by an LSTM Autoencoder model, stored in InfluxDB, visualized with Grafana, and monitored through automated alerting services.

Whenever an anomaly is detected, the system immediately sends notifications through **Email** and **Telegram**, allowing rapid response to abnormal events.

---

# Features

- 📡 Real-time IoT sensor simulation
- ⚡ Apache Kafka streaming pipeline
- 🤖 Deep Learning anomaly detection using LSTM Autoencoder
- 📊 Live Grafana dashboard
- 🗄️ InfluxDB time-series database
- 🚨 Real-time anomaly detection
- 📧 Automatic Email notifications
- 💬 Telegram Bot alerts
- 📈 Live sensor visualization
- 🐳 Dockerized deployment

---

# System Architecture

```
IoT Sensors
      │
      ▼
Python Sensor Simulator
      │
      ▼
Kafka Producer
      │
      ▼
Apache Kafka
      │
      ▼
Processor (Consumer)
      │
      ▼
LSTM Autoencoder
      │
      ├────────► Email Alerts
      │
      ├────────► Telegram Alerts
      │
      ▼
InfluxDB
      │
      ▼
Grafana Dashboard
```

---

# Model

The anomaly detection engine is based on an **LSTM Autoencoder** trained exclusively on normal sensor behavior.

### Detection Pipeline

- Data Collection
- Data Preprocessing
- Data Normalization
- Sequence Generation
- Model Training
- Reconstruction Error Calculation
- Threshold-Based Detection
- Real-Time Anomaly Prediction

---

# Monitored Sensors

- 🌡 Temperature
- 💧 Humidity
- 🔥 Gas
- 📳 Vibration
- 🚭 Smoke

---

# Tech Stack

### Programming Language

- Python

### Libraries

- TensorFlow
- Keras
- NumPy
- Pandas
- Scikit-learn

### Streaming

- Apache Kafka
- Zookeeper

### Database

- InfluxDB

### Visualization

- Grafana

### Notifications

- SMTP (Email Alerts)
- Telegram Bot API

### Containerization

- Docker
- Docker Compose

---

# 📂 Project Structure

```
Real-Time-IoT-Anomaly-Detection/
│
├── config/                  # Configuration files
├── dashboard/               # Dashboard scripts
├── data_generator/          # IoT sensor simulator
├── grafana/                 # Grafana dashboards
├── models/                  # Trained LSTM Autoencoder models
├── processor/               # Kafka consumer & anomaly detection
├── scalers/                 # Saved preprocessing scalers
│
├── Dockerfile
├── Dockerfile.processor
├── Dockerfile.simulator
├── docker-compose.yml
├── requirements.txt
├── Run.ps1
├── Stop.ps1
└── README.md
```

---

# Getting Started

## 1. Clone the Repository

```bash
git clone <repository-url>

cd Real-Time-IoT-Anomaly-Detection
```

---

## 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 3. Start Docker Services

```bash
docker-compose up --build
```

Or simply run

```powershell
.\Run.ps1
```

---

## 4. Stop the Project

```powershell
.\Stop.ps1
```

---

# Grafana Dashboard

The dashboard provides:

- 📈 Live sensor readings
- 🚨 Real-time anomaly monitoring
- 📉 Time-series visualization
- 📊 Historical sensor analysis
- 📍 System performance monitoring

---

# Notification System

When the reconstruction error exceeds the predefined threshold:

- 📧 An email notification is automatically sent.
- 💬 A Telegram Bot instantly sends an alert message.
- 📊 The anomaly is simultaneously visualized in Grafana.

This ensures immediate awareness of abnormal sensor behavior.

---

# 🐳 Docker Services

| Service | Description |
|----------|-------------|
| Kafka | Message Broker |
| Zookeeper | Kafka Coordination |
| InfluxDB | Time-Series Database |
| Grafana | Monitoring Dashboard |
| Data Generator | IoT Sensor Simulator |
| Processor | Real-Time Anomaly Detection |






