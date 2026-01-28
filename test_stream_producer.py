from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Producteur Kafka démarré")

for i in range(50):
    message = {
        "source_type": "iot",
        "machine_id": f"M{(i % 5)+1:03d}",
        "temperature": round(random.uniform(70, 100), 2),
        "pressure": round(random.uniform(140, 200), 2),
        "vibration": round(random.uniform(1, 10), 2),
        "timestamp": datetime.now().isoformat()
    }
    
    producer.send("iot-topic", message)
    print(f"Message {i+1} envoyé")
    time.sleep(1)

producer.flush()
print("50 messages envoyés")
