import json
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, MY_NAME

input_topic = f"building_sensors_{MY_NAME}"
temp_alert_topic = f"temperature_alerts_{MY_NAME}"
humidity_alert_topic = f"humidity_alerts_{MY_NAME}"

consumer = KafkaConsumer(
    input_topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=f"sensor_processor_{MY_NAME}",
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"Очікування повідомлень із топіка '{input_topic}'...")

try:
    for message in consumer:
        data = message.value
        temp = data["temperature"]
        humidity = data["humidity"]
        triggered = False

        if temp > 40:
            triggered = True
            temp_alert = {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "temperature": temp,
                "humidity": humidity,
                "message": f"Температура перевищила поріг 40°C ({temp}°C)",
            }
            print(f"[ФІЛЬТР] Відібрано за температурою: {data}")
            producer.send(temp_alert_topic, value=temp_alert)
            producer.flush()
            print(f"[ВІДПРАВЛЕНО -> {temp_alert_topic}]: {temp_alert}\n")

        if humidity > 80 or humidity < 20:
            triggered = True
            humidity_alert = {
                "sensor_id": data["sensor_id"],
                "timestamp": data["timestamp"],
                "temperature": temp,
                "humidity": humidity,
                "message": f"Вологість вийшла за межі 20–80% ({humidity}%)",
            }
            print(f"[ФІЛЬТР] Відібрано за вологістю: {data}")
            producer.send(humidity_alert_topic, value=humidity_alert)
            producer.flush()
            print(f"[ВІДПРАВЛЕНО -> {humidity_alert_topic}]: {humidity_alert}\n")

        if not triggered:
            print(f"[ПРОПУЩЕНО - показники в нормі]: {data}")

except KeyboardInterrupt:
    print("\nОбробку даних зупинено.")
finally:
    consumer.close()
    producer.close()
