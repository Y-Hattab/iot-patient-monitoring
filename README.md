# IoT Patient Monitoring — Big Data Lab Project

## 1. Project Overview

This project implements a complete Big Data pipeline for real-time IoT patient monitoring.

The system simulates medical IoT sensors attached to patients, streams their readings through Apache Kafka, processes the data with Spark Streaming, stores the results in MongoDB, and triggers alerts when medical thresholds are exceeded.

The final architecture is:

```text
IoT Simulator → Kafka Topics → Spark Streaming → MongoDB + Kafka Alert Topics
```

The project follows the Big Data Lab requirements: Kafka, MongoDB, Spark Streaming, Docker Compose, real-time alerting, and a dataset with at least 10,000 records.

---

## 2. Technologies Used

- Docker Compose
- Apache Kafka
- Zookeeper
- Apache Spark Streaming
- MongoDB
- Mongo Express
- Python
- kafka-python
- PySpark
- MongoDB Spark Connector

---

## 3. Project Structure

```text
iot-patient-monitoring/
│
├── docker-compose.yml
├── README.md
├── .gitignore
│
├── data/
│   ├── generate_dataset.py
│   └── generated_patient_readings.csv
│
├── mongo/
│   └── init_db.js
│
├── producer/
│   ├── iot_simulator.py
│   └── requirements.txt
│
├── scripts/
│   ├── create_topics.py
│   ├── inject_critical_alert.py
│   └── threshold_processor.py
│
└── spark/
    ├── streaming_job.py
    └── requirements.txt
```

### Folder Explanation

| Path | Role |
|---|---|
| `docker-compose.yml` | Starts Kafka, Zookeeper, MongoDB, Mongo Express, Spark Master and Spark Worker. |
| `mongo/init_db.js` | Initializes MongoDB with patients, devices, indexes and collections. |
| `data/generate_dataset.py` | Generates a synthetic dataset of 10,000 patient sensor readings. |
| `producer/iot_simulator.py` | Simulates IoT sensors and sends readings to Kafka topics. |
| `scripts/create_topics.py` | Creates the Kafka topics required by the project. |
| `scripts/inject_critical_alert.py` | Injects a critical medical reading for the live demo. |
| `scripts/threshold_processor.py` | Backup Python processor used during development. |
| `spark/streaming_job.py` | Main Spark Streaming job that reads Kafka, applies thresholds, stores data in MongoDB and sends alerts. |

---

## 4. Dataset Description

The dataset is generated synthetically using Python.

It contains:

- 10,000 sensor readings
- 10 patients
- 5 types of medical metrics
- 5 types of IoT devices

The metrics are:

- `HeartRate`
- `Temperature`
- `OxygenLevel`
- `BloodPressure`
- `RespiratoryRate`

Each record follows this schema:

```json
{
  "timestamp": "2026-01-01T00:00:00+00:00",
  "patientId": "P-0001",
  "deviceId": "DEV-HR-0001",
  "metric": "HeartRate",
  "value": 82.5,
  "unit": "bpm"
}
```

The values are generated to include mostly normal values, some warning values, and a few critical values. This allows testing of the real-time alert system.

To generate the dataset:

```powershell
python data/generate_dataset.py
```

The generated file is:

```text
data/generated_patient_readings.csv
```

---

## 5. Kafka Topics

Kafka is used to transport medical readings in real time between the IoT simulator and Spark Streaming.

A separate topic is used for each medical metric because each metric has its own medical thresholds. Two additional topics are used for warning and critical alerts.

| Topic | Purpose | Partitions |
|---|---|---:|
| `patient.heartrate` | Heart rate readings | 3 |
| `patient.temperature` | Temperature readings | 3 |
| `patient.oxygenlevel` | Oxygen saturation readings | 3 |
| `patient.bloodpressure` | Blood pressure readings | 3 |
| `patient.respiratoryrate` | Respiratory rate readings | 3 |
| `patient.alerts.warning` | Warning alerts | 1 |
| `patient.alerts.critical` | Critical alerts | 1 |

To create the topics:

```powershell
python scripts/create_topics.py
```

To verify the topics:

```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Expected output:

```text
patient.alerts.critical
patient.alerts.warning
patient.bloodpressure
patient.heartrate
patient.oxygenlevel
patient.respiratoryrate
patient.temperature
```

---

## 6. MongoDB Collections

The MongoDB database is named:

```text
patient_monitoring
```

It contains the following collections:

| Collection | Role |
|---|---|
| `patients` | Stores patient information. |
| `devices` | Stores IoT device information. |
| `sensor_readings` | Stores all processed sensor readings with the `alertLevel` field. |
| `alerts` | Stores only warning and critical alerts. |

The `patients` and `devices` collections are initialized automatically through:

```text
mongo/init_db.js
```

---

## 7. Medical Thresholds

Spark Streaming classifies each reading as:

- `normal`
- `warning`
- `critical`

The thresholds used are:

| Metric | Normal | Warning | Critical |
|---|---|---|---|
| `HeartRate` | 60–100 bpm | `< 50` or `> 120` | `< 40` or `> 150` |
| `Temperature` | 36.1–37.2 °C | `> 38.0` | `> 39.5` or `< 35` |
| `OxygenLevel` | 95–100 % | `< 93` | `< 90` |
| `BloodPressure` | 80–120 mmHg | `> 130` | `> 180` or `< 70` |
| `RespiratoryRate` | 12–20 breaths/min | `< 10` or `> 24` | `< 8` or `> 30` |

---

## 8. IoT Simulator Modes

The IoT simulator supports two execution modes.

### Live Mode

In live mode, the simulator generates random medical readings continuously.

```powershell
python producer/iot_simulator.py --mode live --delay 1
```

This mode is useful to simulate active IoT sensors sending real-time data.

### CSV Mode

In CSV mode, the simulator reads the generated dataset and publishes each row to the appropriate Kafka topic.

```powershell
python producer/iot_simulator.py --mode csv --delay 0.1 --limit 50
```

This mode is recommended for the demo because it proves that the dataset contains 10,000 records while still behaving like a real-time stream.

To stream the full dataset:

```powershell
python producer/iot_simulator.py --mode csv --delay 0.01
```

---

## 9. Installation and Execution Guide

### Step 1 — Start Docker Services

```powershell
docker compose up -d
```

Check running containers:

```powershell
docker ps
```

Expected containers:

```text
zookeeper
kafka
mongodb
mongo-express
spark-master
spark-worker
```

---

### Step 2 — Access Web Interfaces

Mongo Express:

```text
http://localhost:8081
```

Login:

```text
username: admin
password: admin
```

Spark Master UI:

```text
http://localhost:8080
```

---

### Step 3 — Install Python Dependencies

```powershell
python -m pip install kafka-python faker numpy pandas pymongo
```

---

### Step 4 — Generate the Dataset

```powershell
python data/generate_dataset.py
```

---

### Step 5 — Create Kafka Topics

```powershell
python scripts/create_topics.py
```

Verify topics:

```powershell
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

### Step 6 — Start Spark Streaming Job

Run this command in one line:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.mongodb.spark:mongo-spark-connector_2.13:10.5.0 /opt/spark-apps/streaming_job.py
```

The Spark job will:

- read Kafka messages
- parse JSON records
- classify each reading
- write all readings to MongoDB
- write alerts to MongoDB
- publish alerts to Kafka alert topics

---

### Step 7 — Start the IoT Simulator

Live mode:

```powershell
python producer/iot_simulator.py --mode live --delay 1
```

CSV mode with 50 records:

```powershell
python producer/iot_simulator.py --mode csv --delay 0.1 --limit 50
```

CSV mode with the full dataset:

```powershell
python producer/iot_simulator.py --mode csv --delay 0.01
```

---

### Step 8 — Inject a Critical Alert

While Spark Streaming is running, execute:

```powershell
python scripts/inject_critical_alert.py
```

This injects:

```text
OxygenLevel = 85.0
```

Since `OxygenLevel` is critical below 90, Spark classifies it as:

```text
critical
```

The alert appears in:

```text
MongoDB → patient_monitoring → alerts
Kafka topic → patient.alerts.critical
```

---

### Step 9 — View Critical Alerts from Kafka

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic patient.alerts.critical --from-beginning
```

---

## 10. Demo Scenario

For the live demo:

1. Show running containers:

```powershell
docker ps
```

2. Show Spark UI:

```text
http://localhost:8080
```

3. Show Mongo Express:

```text
http://localhost:8081
```

4. Create or verify Kafka topics:

```powershell
python scripts/create_topics.py
```

5. Start Spark Streaming:

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.mongodb.spark:mongo-spark-connector_2.13:10.5.0 /opt/spark-apps/streaming_job.py
```

6. Start the IoT simulator in CSV mode:

```powershell
python producer/iot_simulator.py --mode csv --delay 0.1 --limit 50
```

7. Show records appearing in the Spark console.

8. Show records stored in MongoDB:

```text
patient_monitoring → sensor_readings
```

9. Inject a critical alert:

```powershell
python scripts/inject_critical_alert.py
```

10. Show the alert in:

```text
patient_monitoring → alerts
patient.alerts.critical
```

---

## 11. Stop the Project

Stop running Python or Spark scripts with:

```text
CTRL + C
```

Stop Docker containers:

```powershell
docker compose down
```

Stop containers and remove volumes:

```powershell
docker compose down -v
```

Use `down -v` only when you want to reset MongoDB data.

---

## 12. Notes

The file:

```text
scripts/threshold_processor.py
```

is kept as a backup processor used during development.

The final version of the project uses Spark Streaming through:

```text
spark/streaming_job.py
```