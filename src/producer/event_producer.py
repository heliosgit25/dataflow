import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch"]
users = [100, 101, 102, 103, 104, 105, 106, 107, 108, 109]

def generate_event():
    return {
        "user_id": random.choice(users),
        "product": random.choice(products),
        "price": round(random.uniform(100.0, 100000.0), 2),
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    try:
        while True:
            event = generate_event()
            producer.send("ecommerce-events", event)
            print("Sent:", event)
            #print(json.dumps(event))
            time.sleep(5)  # Simulate a delay between events
    except KeyboardInterrupt:
        print("\nProducer stopping...")
