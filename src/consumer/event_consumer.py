from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime

consumer = KafkaConsumer(
    "ecommerce-events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="ecommerce-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# DB Connection
conn = psycopg2.connect(
    dbname="ecommerce",
    user="helios",
    password="",
    host="localhost",
    port="5432"
)

conn.autocommit = True
cursor = conn.cursor()

print("Consumer started...")

for message in consumer:
    event = message.value
    print("Received:", event)
    event_time = datetime.fromisoformat(event["timestamp"])
    cursor.execute("""
    INSERT INTO user_events (user_id, product, price, event_time)
    VALUES (%s, %s, %s, %s)
    """, (
    event["user_id"],
    event["product"],
    event["price"],
    event_time
    ))