from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "ecommerce-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ecommerce-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started...")

for message in consumer:
    event = message.value
    print("Received:", event)