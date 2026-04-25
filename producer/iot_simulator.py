import argparse
import csv
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"
CSV_FILE = "data/generated_patient_readings.csv"

TOPICS = {
    "HeartRate": "patient.heartrate",
    "Temperature": "patient.temperature",
    "OxygenLevel": "patient.oxygenlevel",
    "BloodPressure": "patient.bloodpressure",
    "RespiratoryRate": "patient.respiratoryrate",
}

UNITS = {
    "HeartRate": "bpm",
    "Temperature": "°C",
    "OxygenLevel": "SpO2 %",
    "BloodPressure": "mmHg",
    "RespiratoryRate": "breaths/min",
}

PATIENTS = [
    "P-0001", "P-0002", "P-0003", "P-0004", "P-0005",
    "P-0006", "P-0007", "P-0008", "P-0009", "P-0010"
]

DEVICE_PREFIX = {
    "HeartRate": "DEV-HR",
    "Temperature": "DEV-TEMP",
    "OxygenLevel": "DEV-OX",
    "BloodPressure": "DEV-BP",
    "RespiratoryRate": "DEV-RR",
}


def generate_value(metric: str) -> float:
    risk = random.random()

    if metric == "HeartRate":
        if risk < 0.80:
            return round(random.uniform(60, 100), 1)
        if risk < 0.95:
            return round(random.choice([random.uniform(45, 49), random.uniform(121, 140)]), 1)
        return round(random.choice([random.uniform(30, 39), random.uniform(151, 180)]), 1)

    if metric == "Temperature":
        if risk < 0.80:
            return round(random.uniform(36.1, 37.2), 1)
        if risk < 0.95:
            return round(random.uniform(38.1, 39.4), 1)
        return round(random.choice([random.uniform(34.0, 34.9), random.uniform(39.6, 41.0)]), 1)

    if metric == "OxygenLevel":
        if risk < 0.80:
            return round(random.uniform(95, 100), 1)
        if risk < 0.95:
            return round(random.uniform(90, 92.9), 1)
        return round(random.uniform(80, 89.9), 1)

    if metric == "BloodPressure":
        if risk < 0.80:
            return round(random.uniform(80, 120), 1)
        if risk < 0.95:
            return round(random.uniform(131, 170), 1)
        return round(random.choice([random.uniform(50, 69), random.uniform(181, 220)]), 1)

    if metric == "RespiratoryRate":
        if risk < 0.80:
            return round(random.uniform(12, 20), 1)
        if risk < 0.95:
            return round(random.choice([random.uniform(9, 9.9), random.uniform(24.1, 29)]), 1)
        return round(random.choice([random.uniform(5, 7.9), random.uniform(31, 40)]), 1)

    raise ValueError(f"Unknown metric: {metric}")


def generate_reading() -> dict:
    metric = random.choice(list(TOPICS.keys()))
    patient_id = random.choice(PATIENTS)
    patient_number = patient_id.split("-")[1]
    device_id = f"{DEVICE_PREFIX[metric]}-{patient_number}"

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "patientId": patient_id,
        "deviceId": device_id,
        "metric": metric,
        "value": generate_value(metric),
        "unit": UNITS[metric],
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_reading(producer: KafkaProducer, reading: dict):
    metric = reading["metric"]
    topic = TOPICS[metric]

    reading["value"] = float(reading["value"])

    producer.send(topic, reading)
    producer.flush()

    print(f"Sent to {topic}: {reading}")


def run_live_mode(delay: float):
    producer = create_producer()

    print("IoT Simulator started in LIVE mode.")
    print("Generating readings in real time. Press CTRL+C to stop.")

    try:
        while True:
            reading = generate_reading()
            send_reading(producer, reading)
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\nLive simulator stopped.")

    finally:
        producer.close()


def run_csv_mode(delay: float, limit: int | None):
    producer = create_producer()

    print("IoT Simulator started in CSV mode.")
    print(f"Reading dataset from: {CSV_FILE}")
    print("Sending CSV records to Kafka. Press CTRL+C to stop.")

    sent_count = 0

    try:
        with open(CSV_FILE, mode="r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(csv_file)

            for row in reader:
                send_reading(producer, row)
                sent_count += 1

                if limit is not None and sent_count >= limit:
                    break

                time.sleep(delay)

        print(f"\nCSV simulation completed. Records sent: {sent_count}")

    except FileNotFoundError:
        print(f"ERROR: CSV file not found: {CSV_FILE}")
        print("Run this command first:")
        print("python data/generate_dataset.py")

    except KeyboardInterrupt:
        print(f"\nCSV simulator stopped. Records sent: {sent_count}")

    finally:
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="IoT Patient Monitoring Simulator")

    parser.add_argument(
        "--mode",
        choices=["live", "csv"],
        default="live",
        help="Simulation mode: live generates data in real time, csv reads from generated dataset."
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between two Kafka messages."
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of CSV records to send. Only used in csv mode."
    )

    args = parser.parse_args()

    if args.mode == "live":
        run_live_mode(args.delay)
    else:
        run_csv_mode(args.delay, args.limit)


if __name__ == "__main__":
    main()