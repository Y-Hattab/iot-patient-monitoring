import json
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient


BOOTSTRAP_SERVERS = "localhost:9092"

INPUT_TOPICS = [
    "patient.heartrate",
    "patient.temperature",
    "patient.oxygenlevel",
    "patient.bloodpressure",
    "patient.respiratoryrate",
]

WARNING_TOPIC = "patient.alerts.warning"
CRITICAL_TOPIC = "patient.alerts.critical"

MONGO_URI = "mongodb://root:root@localhost:27017/?authSource=admin"
DATABASE_NAME = "patient_monitoring"


def classify_reading(metric, value):
    """
    Classify each medical reading according to the project thresholds.
    Returns: normal, warning, or critical.
    """

    if metric == "HeartRate":
        if value < 40 or value > 150:
            return "critical"
        if value < 50 or value > 120:
            return "warning"
        return "normal"

    if metric == "Temperature":
        if value > 39.5 or value < 35:
            return "critical"
        if value > 38.0:
            return "warning"
        return "normal"

    if metric == "OxygenLevel":
        if value < 90:
            return "critical"
        if value < 93:
            return "warning"
        return "normal"

    if metric == "BloodPressure":
        if value > 180 or value < 70:
            return "critical"
        if value > 130:
            return "warning"
        return "normal"

    if metric == "RespiratoryRate":
        if value < 8 or value > 30:
            return "critical"
        if value < 10 or value > 24:
            return "warning"
        return "normal"

    return "normal"


def main():
    print("Starting threshold processor...")
    print("Reading from Kafka topics:")
    for topic in INPUT_TOPICS:
        print(f"- {topic}")

    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="threshold-processor-group",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DATABASE_NAME]

    sensor_readings_collection = db["sensor_readings"]
    alerts_collection = db["alerts"]

    print("Processor is running. Press CTRL+C to stop.")

    try:
        for message in consumer:
            reading = message.value

            metric = reading["metric"]
            value = float(reading["value"])
            alert_level = classify_reading(metric, value)

            reading["alertLevel"] = alert_level
            reading["processedAt"] = datetime.utcnow().isoformat() + "Z"

            sensor_readings_collection.insert_one(reading.copy())

            print(f"[{alert_level.upper()}] {metric} = {value} for patient {reading['patientId']}")

            if alert_level in ["warning", "critical"]:
                alert = {
                    "timestamp": reading["timestamp"],
                    "processedAt": reading["processedAt"],
                    "patientId": reading["patientId"],
                    "deviceId": reading["deviceId"],
                    "metric": metric,
                    "value": value,
                    "unit": reading["unit"],
                    "alertLevel": alert_level,
                    "message": f"{alert_level.upper()} alert detected for {metric}",
                }

                alerts_collection.insert_one(alert.copy())

                alert_topic = CRITICAL_TOPIC if alert_level == "critical" else WARNING_TOPIC
                producer.send(alert_topic, alert)
                producer.flush()

                print(f"Alert sent to Kafka topic: {alert_topic}")

    except KeyboardInterrupt:
        print("\nProcessor stopped.")

    finally:
        consumer.close()
        producer.close()
        mongo_client.close()


if __name__ == "__main__":
    main()