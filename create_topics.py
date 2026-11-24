from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config, MY_NAME

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
)

topics = [
    NewTopic(
        name=f"building_sensors_{MY_NAME}", num_partitions=2, replication_factor=1
    ),
    NewTopic(
        name=f"temperature_alerts_{MY_NAME}", num_partitions=2, replication_factor=1
    ),
    NewTopic(name=f"humidity_alerts_{MY_NAME}", num_partitions=2, replication_factor=1),
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Топіки успішно створено.")
except TopicAlreadyExistsError:
    print("Топіки вже існують.")

[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
