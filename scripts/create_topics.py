from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = "localhost:9092"

topics = [
    NewTopic(name="patient.heartrate", num_partitions=3, replication_factor=1),
    NewTopic(name="patient.temperature", num_partitions=3, replication_factor=1),
    NewTopic(name="patient.oxygenlevel", num_partitions=3, replication_factor=1),
    NewTopic(name="patient.bloodpressure", num_partitions=3, replication_factor=1),
    NewTopic(name="patient.respiratoryrate", num_partitions=3, replication_factor=1),
    NewTopic(name="patient.alerts.warning", num_partitions=1, replication_factor=1),
    NewTopic(name="patient.alerts.critical", num_partitions=1, replication_factor=1),
]


def main():
    admin_client = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="patient-monitoring-admin"
    )

    existing_topics = admin_client.list_topics()
    topics_to_create = [topic for topic in topics if topic.name not in existing_topics]

    if not topics_to_create:
        print("All Kafka topics already exist.")
        admin_client.close()
        return

    try:
        admin_client.create_topics(
            new_topics=topics_to_create,
            validate_only=False
        )
        print("Kafka topics created successfully:")
        for topic in topics_to_create:
            print(f"- {topic.name}")

    except TopicAlreadyExistsError:
        print("Some topics already exist.")

    finally:
        admin_client.close()


if __name__ == "__main__":
    main()