from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time

print("Test Kafka...")

try:
    admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
    
    topic = NewTopic(name="iot-topic", num_partitions=1, replication_factor=1)
    admin.create_topics([topic])
    print(" Topic iot-topic créé")
    
except:
    print("Topic existe déjà")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

test_data = {
    "source_type": "test",
    "machine_id": "TEST001",
    "temperature": 75.5,
    "pressure": 150.2,
    "vibration": 3.1,
    "timestamp": "2024-01-28T10:00:00"
}

producer.send("iot-topic", test_data)
producer.flush()

print(" Message envoyé à Kafka")
print(" Kafka fonctionne correctement")
