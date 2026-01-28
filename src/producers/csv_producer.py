import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime

class CSVProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'iot-topic'
    
    def stream_csv_data(self, csv_file):
        df = pd.read_csv(csv_file)
        
        for index, row in df.iterrows():
            message = {
                'source_type': 'csv',
                'machine_id': row['machine_id'],
                'temperature': float(row['temperature']),
                'pressure': float(row['pressure']),
                'vibration': float(row['vibration']),
                'location': row['location'],
                'timestamp': datetime.now().isoformat()
            }
            
            self.producer.send(self.topic, message)
            print(f"CSV data sent: {message}")
            time.sleep(2)

if __name__ == "__main__":
    producer = CSVProducer()
    producer.stream_csv_data('data/sources/historical_data.csv')
