from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

print("Test Producteur Kafka")
for i in range(3):
    data = {"test": i, "value": random.randint(1, 100)}
    producer.send("iot_sensor_data", json.dumps(data).encode())
    print("Message " + str(i) + " envoye")
    time.sleep(1)

print("Test reussi")
