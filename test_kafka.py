from kafka import KafkaProducer, KafkaConsumer
import json
import time

try:
    print("Test Kafkaa")
    
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    
    test_msg = {"test": "message", "time": "now"}
    producer.send("test-topic", test_msg)
    producer.flush()
    
    print(" Producer OK")
    
    consumer = KafkaConsumer(
        "test-topic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=2000
    )
    
    for msg in consumer:
        print(f" Consumer OK - Message: {msg.value.decode()}")
        break
    
    print(" Kafka fonctionne")
    
except Exception as e:
    print(f" Kafka erreur: {e}")
    print("Kafka ne démarre pas. Essayons docker-compose.yml original")
