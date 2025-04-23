from kafka import KafkaConsumer
import json


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = 'news-events'

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers = KAFKA_SERVER,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
for message in consumer:
    print(f"{message.value}")
