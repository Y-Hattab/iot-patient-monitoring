# IoT Patient Monitoring — Big Data Lab Project

## 1. Project Overview

This project implements a complete Big Data pipeline for real-time IoT patient monitoring.

The system simulates medical IoT sensors attached to patients, streams their readings through Apache Kafka, processes the data with Spark Streaming, stores the results in MongoDB, and triggers alerts when medical thresholds are exceeded.

The final architecture is:

```text
IoT Simulator → Kafka Topics → Spark Streaming → MongoDB + Kafka Alert Topics
```
The project follows the requirements of the Big Data Lab statement: Kafka, MongoDB, Spark Streaming, Docker Compose, real-time alerting, and a dataset with at least 10,000 records.

## 2. Technologies Used
Docker Compose
Apache Kafka
Zookeeper
Apache Spark Streaming
MongoDB
Mongo Express
Python
kafka-python
PySpark
MongoDB Spark Connector
## 3. Project Structure
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
Folder explanation
docker-compose.yml: starts Kafka, Zookeeper, MongoDB, Mongo Express, Spark master and Spark worker.
mongo/init_db.js: initializes MongoDB with patients, devices, indexes, and collections.
data/generate_dataset.py: generates a synthetic dataset of 10,000 patient sensor readings.
producer/iot_simulator.py: simulates IoT sensors and sends readings to Kafka topics.
scripts/create_topics.py: creates the Kafka topics required by the project.
scripts/inject_critical_alert.py: injects a critical medical reading for the live demo.
scripts/threshold_processor.py: backup Python processor used during development.
spark/streaming_job.py: main Spark Streaming job that reads Kafka, applies thresholds, stores data in MongoDB, and sends alerts.
## 4. Dataset Description

The dataset is generated synthetically using Python.

It contains:

10,000 sensor readings
10 patients
5 types of medical metrics
5 types of IoT devices

The metrics are:

HeartRate
Temperature
OxygenLevel
BloodPressure
RespiratoryRate

Each record follows this schema:

{
  "timestamp": "2026-01-01T00:00:00+00:00",
  "patientId": "P-0001",
  "deviceId": "DEV-HR-0001",
  "metric": "HeartRate",
  "value": 82.5,
  "unit": "bpm"
}

The values are generated to include mostly normal values, some warning values, and a few critical values. This allows testing of the real-time alert system.

## 5. Kafka Topics

The following Kafka topics are used:

Topic	Purpose	Partitions
patient.heartrate	Heart rate readings	3
patient.temperature	Temperature readings	3
patient.oxygenlevel	Oxygen saturation readings	3
patient.bloodpressure	Blood pressure readings	3
patient.respiratoryrate	Respiratory rate readings	3
patient.alerts.warning	Warning alerts	1
patient.alerts.critical	Critical alerts	1

A separate topic is used for each medical metric because each metric has different medical thresholds. Alert topics are separated by severity level.

## 6. MongoDB Collections

The MongoDB database is named:

patient_monitoring

It contains the following collections:

patients

Stores patient information.

devices

Stores IoT device information.

sensor_readings

Stores all processed sensor readings, including the alertLevel field.

alerts

Stores only warning and critical alerts.

## 7. Medical Thresholds

Spark Streaming classifies each reading as:

normal
warning
critical

The thresholds used are:

Metric	Normal	Warning	Critical
HeartRate	60–100 bpm	< 50 or > 120	< 40 or > 150
Temperature	36.1–37.2 °C	> 38.0	> 39.5 or < 35
OxygenLevel	95–100 %	< 93	< 90
BloodPressure	80–120 mmHg	> 130	> 180 or < 70
RespiratoryRate	12–20 breaths/min	< 10 or > 24	< 8 or > 30
## 8. Installation and Execution Guide
Step 1 — Start Docker services
docker compose up -d

Check running containers:

docker ps

Expected containers:

zookeeper
kafka
mongodb
mongo-express
spark-master
spark-worker
Step 2 — Access web interfaces

Mongo Express:

http://localhost:8081

Login:

username: admin
password: admin

Spark Master UI:

http://localhost:8080
Step 3 — Install Python dependencies
python -m pip install kafka-python faker numpy pandas pymongo
Step 4 — Generate the dataset
python data/generate_dataset.py

This generates:

data/generated_patient_readings.csv
Step 5 — Create Kafka topics
python scripts/create_topics.py

Verify topics:

docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list

Expected topics:

patient.alerts.critical
patient.alerts.warning
patient.bloodpressure
patient.heartrate
patient.oxygenlevel
patient.respiratoryrate
patient.temperature
Step 6 — Start Spark Streaming job

Run this command in one line:

docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.mongodb.spark:mongo-spark-connector_2.13:10.5.0 /opt/spark-apps/streaming_job.py

The Spark job will:

read Kafka messages
parse JSON records
classify each reading
write all readings to MongoDB
write alerts to MongoDB
publish alerts to Kafka alert topics
Step 7 — Start the IoT simulator

Live mode:

python producer/iot_simulator.py --mode live --delay 1

CSV mode with 50 records:

python producer/iot_simulator.py --mode csv --delay 0.1 --limit 50

CSV mode with the full dataset:

python producer/iot_simulator.py --mode csv --delay 0.01
Step 8 — Inject a critical alert

While Spark Streaming is running, execute:

python scripts/inject_critical_alert.py

This injects:

OxygenLevel = 85.0

Since OxygenLevel is critical below 90, Spark should classify it as:

critical

The alert should appear in:

MongoDB → patient_monitoring → alerts
Kafka topic → patient.alerts.critical
Step 9 — View critical alerts from Kafka
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic patient.alerts.critical --from-beginning
## 9. Demo Scenario

For the live demo:

Show running containers:
docker ps
Show Spark UI:
http://localhost:8080
Show Mongo Express:
http://localhost:8081
Create or verify Kafka topics:
python scripts/create_topics.py
Start Spark Streaming:
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.mongodb.spark:mongo-spark-connector_2.13:10.5.0 /opt/spark-apps/streaming_job.py
Start the IoT simulator:
python producer/iot_simulator.py --mode csv --delay 0.1 --limit 50
Show records appearing in Spark console.
Show records stored in MongoDB:
patient_monitoring → sensor_readings
Inject a critical alert:
python scripts/inject_critical_alert.py
Show the alert in:
patient_monitoring → alerts
patient.alerts.critical
## 10. Stop the Project

Stop running Python scripts with:

CTRL + C

Stop Docker containers:

docker compose down

Stop and remove volumes:

docker compose down -v

Use down -v only when you want to reset MongoDB data.

## 11. Notes

The file scripts/threshold_processor.py is kept as a backup processor used during development. The final version of the project uses Spark Streaming through:

spark/streaming_job.py

