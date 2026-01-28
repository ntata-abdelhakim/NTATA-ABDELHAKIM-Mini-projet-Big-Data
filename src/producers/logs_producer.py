import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

class LogsProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'iot-topic'
        
        self.log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG']
        self.components = ['auth', 'database', 'api', 'network', 'security']
        self.messages = [
            "User login successful",
            "Database connection lost",
            "API response time high",
            "Network latency detected",
            "Security alert: multiple failed attempts",
            "Memory usage critical",
            "CPU overload warning",
            "Disk space low",
            "Backup completed",
            "System update required"
        ]
    
    def generate_log_entry(self):
        log_entry = {
            'source_type': 'system_logs',
            'timestamp': datetime.now().isoformat(),
            'log_level': random.choice(self.log_levels),
            'component': random.choice(self.components),
            'message': random.choice(self.messages),
            'user_id': f"user_{random.randint(1000, 9999)}",
            'session_id': f"session_{random.randint(10000, 99999)}",
            'ip_address': f"192.168.{random.randint(1,255)}.{random.randint(1,255)}"
        }
        
        self.producer.send(self.topic, log_entry)
        print(f"Log sent: {log_entry}")
    
    def start_logging(self, interval=3):
        while True:
            self.generate_log_entry()
            time.sleep(interval)

if __name__ == "__main__":
    producer = LogsProducer()
    producer.start_logging(interval=3)
