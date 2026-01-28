from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

print("Demarrage producteur...")

try:
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    print("Producteur initialise")
    
    machine_ids = ["M001", "M002", "M003"]
    
    for i in range(10):
        message = {
            "source_type": "iot",
            "machine_id": random.choice(machine_ids),
            "temperature": round(random.uniform(70, 100), 2),
            "pressure": round(random.uniform(140, 200), 2),
            "vibration": round(random.uniform(1, 10), 2),
            "timestamp": datetime.now().isoformat()
        }
        
        producer.send("iot-topic", message)
        print(f"Message {i+1} envoye")
        
        time.sleep(1)
    
    producer.flush()
    print("10 messages envoyes")
    
except Exception as e:
    print(f"Erreur: {e}")
