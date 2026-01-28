import sys
import os
sys.path.append('src')
from database.db_handler import db

from kafka import KafkaConsumer
import json
import pandas as pd
import time

class IoTConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "iot_sensor_data",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="iot_enhanced_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        self.data = []

    def analyze_batch(self, batch):
        if not batch:
            return

        df = pd.DataFrame(batch)

        print("\n" + "=" * 40)
        print("BATCH ANALYSE: " + str(len(df)) + " messages")
        print("-" * 40)

        print("Machines: " + ", ".join(df["machine_id"].unique()))
        print("Temperature moyenne: " + str(round(df["temperature"].mean(), 1)) + "C")
        print("Pression moyenne: " + str(round(df["pressure"].mean(), 1)) + " PSI")

        for index, row in df.iterrows():
            is_anomaly = row["temperature"] > 90 or row["pressure"] > 180
            anomaly_type = None
            if row["temperature"] > 90:
                anomaly_type = "TEMPERATURE"
            elif row["pressure"] > 180:
                anomaly_type = "PRESSURE"
            
            db.save_measurement(
                machine_id=row["machine_id"],
                temperature=row["temperature"],
                pressure=row["pressure"],
                vibration=row.get("vibration", 0),
                is_anomaly=is_anomaly,
                anomaly_type=anomaly_type
            )
            
            if is_anomaly:
                db.save_alert(
                    machine_id=row["machine_id"],
                    alert_type=anomaly_type,
                    value=row["temperature"] if anomaly_type == "TEMPERATURE" else row["pressure"],
                    threshold=90 if anomaly_type == "TEMPERATURE" else 180,
                    severity="CRITICAL"
                )

        high_temp = df[df["temperature"] > 90]
        high_pressure = df[df["pressure"] > 180]

        if len(high_temp) > 0:
            print("ALERTE TEMPERATURE: " + str(len(high_temp)) + " mesures > 90C")
        if len(high_pressure) > 0:
            print("ALERTE PRESSION: " + str(len(high_pressure)) + " mesures > 180 PSI")

        print("=" * 40)

    def run(self, duration_seconds=20):
        print("CONSOMMATEUR IoT - SURVEILLANCE")
        print("Lecture des donnees pendant " + str(duration_seconds) + " secondes")
        print("-" * 40)

        start_time = time.time()
        batch = []
        batch_size = 5

        try:
            for message in self.consumer:
                batch.append(message.value)

                if len(batch) >= batch_size:
                    self.analyze_batch(batch)
                    batch = []

                if time.time() - start_time >= duration_seconds:
                    print("Duree ecoulee - Arret du consommateur")
                    break

        except KeyboardInterrupt:
            print("Arret demande")
        finally:
            self.consumer.close()

            if batch:
                self.analyze_batch(batch)

            print("\nSURVEILLANCE TERMINEE")
            print("Temps total: " + str(round(time.time() - start_time, 1)) + "s")

if __name__ == "__main__":
    consumer = IoTConsumer()
    consumer.run(duration_seconds=15)



def process_csv_data(data):
    import sqlite3
    conn = sqlite3.connect('data/iot_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO csv_data 
    (machine_id, temperature, pressure, vibration, location, batch_id, timestamp, source_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('machine_id'),
        data.get('temperature'),
        data.get('pressure'),
        data.get('vibration'),
        data.get('location'),
        data.get('batch_id'),
        data.get('timestamp'),
        data.get('source_type')
    ))
    
    conn.commit()
    conn.close()
    print(f"CSV data saved: {data.get('machine_id')}")

def process_api_data(data):
    import sqlite3
    conn = sqlite3.connect('data/iot_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO api_data 
    (sensor_id, value, unit, status, api_version, response_time, city, timestamp, source_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('sensor_id'),
        data.get('value'),
        data.get('unit'),
        data.get('status'),
        data.get('api_version'),
        data.get('response_time'),
        data.get('city'),
        data.get('timestamp'),
        data.get('source_type')
    ))
    
    conn.commit()
    conn.close()
    print(f"API data saved: {data.get('sensor_id')}")

def process_log_data(data):
    import sqlite3
    conn = sqlite3.connect('data/iot_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO system_logs 
    (log_level, component, message, user_id, session_id, ip_address, timestamp, source_type)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data.get('log_level'),
        data.get('component'),
        data.get('message'),
        data.get('user_id'),
        data.get('session_id'),
        data.get('ip_address'),
        data.get('timestamp'),
        data.get('source_type')
    ))
    
    conn.commit()
    conn.close()
    print(f"Log saved: {data.get('log_level')} - {data.get('message')}")

def process_message(message):
    import json
    try:
        data = json.loads(message.value.decode('utf-8'))
        source_type = data.get('source_type', 'unknown')
        
        if source_type in ['iot', 'iot_simulated']:
            process_iot_data(data)
        elif source_type == 'csv' or source_type == 'csv_simulated':
            process_csv_data(data)
        elif 'api' in source_type:
            process_api_data(data)
        elif 'log' in source_type:
            process_log_data(data)
        else:
            print(f"Unknown source type: {source_type}")
            
    except Exception as e:
        print(f"Error processing message: {e}")
