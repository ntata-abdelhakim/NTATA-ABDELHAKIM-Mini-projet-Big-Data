import sqlite3
import json
from datetime import datetime
import pandas as pd
import os

class IoTDatabase:
    def __init__(self, db_path="data/iot_data.db"):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS machines (
            machine_id TEXT PRIMARY KEY,
            location TEXT,
            type TEXT,
            installation_date TEXT,
            last_maintenance TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS iot_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            machine_id TEXT,
            temperature REAL,
            pressure REAL,
            vibration REAL,
            is_anomaly BOOLEAN,
            anomaly_type TEXT,
            FOREIGN KEY (machine_id) REFERENCES machines(machine_id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            machine_id TEXT,
            alert_type TEXT,
            value REAL,
            threshold REAL,
            severity TEXT,
            resolved BOOLEAN DEFAULT 0,
            resolved_at DATETIME
        )
        ''')
        
        self.conn.commit()
    
    def save_measurement(self, machine_id, temperature, pressure, vibration, is_anomaly=False, anomaly_type=None):
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO iot_measurements 
        (machine_id, temperature, pressure, vibration, is_anomaly, anomaly_type)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (machine_id, temperature, pressure, vibration, is_anomaly, anomaly_type))
        self.conn.commit()
        return cursor.lastrowid
    
    def save_alert(self, machine_id, alert_type, value, threshold, severity="HIGH"):
        cursor = self.conn.cursor()
        cursor.execute('''
        INSERT INTO alerts (machine_id, alert_type, value, threshold, severity)
        VALUES (?, ?, ?, ?, ?)
        ''', (machine_id, alert_type, value, threshold, severity))
        self.conn.commit()
    
    def get_recent_data(self, limit=100):
        query = '''
        SELECT * FROM iot_measurements 
        ORDER BY timestamp DESC 
        LIMIT ?
        '''
        return pd.read_sql_query(query, self.conn, params=(limit,))
    
    def get_machine_stats(self, machine_id):
        query = '''
        SELECT 
            machine_id,
            COUNT(*) as total_measurements,
            AVG(temperature) as avg_temperature,
            AVG(pressure) as avg_pressure,
            SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomaly_count
        FROM iot_measurements
        WHERE machine_id = ?
        GROUP BY machine_id
        '''
        return pd.read_sql_query(query, self.conn, params=(machine_id,))
    
    def close(self):
        self.conn.close()

db = IoTDatabase()
