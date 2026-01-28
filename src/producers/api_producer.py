import requests
import time
import json
from kafka import KafkaProducer
from datetime import datetime

class APIProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'iot-topic'
    
    def fetch_dummy_data(self, city="Paris"):
        import random
        
        message = {
            'source_type': 'api_simulated',
            'city': city,
            'temperature': round(random.uniform(10, 35), 1),
            'pressure': random.randint(980, 1030),
            'humidity': random.randint(30, 90),
            'wind_speed': round(random.uniform(0, 20), 1),
            'timestamp': datetime.now().isoformat()
        }
        
        self.producer.send(self.topic, message)
        print(f"Weather data sent: {message}")
    
    def start_streaming(self, cities=None, interval=30):
        if cities is None:
            cities = ["Paris", "London", "Berlin", "Madrid", "Rome"]
        
        while True:
            for city in cities:
                self.fetch_dummy_data(city)
                time.sleep(5)
            time.sleep(interval)

if __name__ == "__main__":
    producer = APIProducer()
    producer.start_streaming(interval=60)
