import json
from datetime import datetime, timezone

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = "localhost:9092"

critical_reading = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "patientId": "P-0001",
    "deviceId": "DEV-OX-0001",
    "metric": "OxygenLevel",
    "value": 85.0,
    "unit": "SpO2 %",
}

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

producer.send("patient.oxygenlevel", critical_reading)
producer.flush()
producer.close()

print("Critical alert injected successfully:")
print(critical_reading)