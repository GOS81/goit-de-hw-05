import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from configs import kafka_config, MY_NAME

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

topic_name = f"building_sensors_{MY_NAME}"
sensor_id = random.randint(1000, 9999)

print(f"Датчик #{sensor_id} запущено. Відправка даних у топік '{topic_name}'...")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": round(random.uniform(25, 45), 2),
            "humidity": round(random.uniform(15, 85), 2),
        }
        producer.send(topic_name, value=data)
        producer.flush()
        print(f"Відправлено: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print(f"\nДатчик #{sensor_id} зупинено.")
finally:
    producer.close()
