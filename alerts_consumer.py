import json
from kafka import KafkaConsumer
from configs import kafka_config, MY_NAME

temp_alert_topic = f"temperature_alerts_{MY_NAME}"
humidity_alert_topic = f"humidity_alerts_{MY_NAME}"

consumer = KafkaConsumer(
    temp_alert_topic,
    humidity_alert_topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"alerts_reader_{MY_NAME}",
)

print(
    f"Підписано на топіки '{temp_alert_topic}' та '{humidity_alert_topic}'. Очікування сповіщень...\n"
)

try:
    for message in consumer:
        alert = message.value
        print(
            f"[{message.topic}] Сповіщення від датчика #{alert['sensor_id']} ({alert['timestamp']}): {alert['message']}"
        )
except KeyboardInterrupt:
    print("\nЗчитування сповіщень зупинено.")
finally:
    consumer.close()
