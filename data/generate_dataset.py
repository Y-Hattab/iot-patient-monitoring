import csv
import random
from datetime import datetime, timedelta, timezone


OUTPUT_FILE = "data/generated_patient_readings.csv"
NUMBER_OF_RECORDS = 10000

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


def generate_record(index: int) -> dict:
    metric = random.choice(list(TOPICS.keys()))
    patient_id = random.choice(PATIENTS)
    patient_number = patient_id.split("-")[1]
    device_id = f"{DEVICE_PREFIX[metric]}-{patient_number}"

    start_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    timestamp = start_date + timedelta(seconds=index * 10)

    return {
        "timestamp": timestamp.isoformat(),
        "patientId": patient_id,
        "deviceId": device_id,
        "metric": metric,
        "value": generate_value(metric),
        "unit": UNITS[metric],
    }


def main():
    fieldnames = ["timestamp", "patientId", "deviceId", "metric", "value", "unit"]

    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(NUMBER_OF_RECORDS):
            writer.writerow(generate_record(i))

    print(f"Dataset generated successfully: {OUTPUT_FILE}")
    print(f"Number of records: {NUMBER_OF_RECORDS}")
    print(f"Patients: {len(PATIENTS)}")
    print(f"Metrics: {', '.join(TOPICS.keys())}")


if __name__ == "__main__":
    main()