import sqlite3

conn = sqlite3.connect('data/iot_data.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS csv_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    machine_id TEXT,
    temperature REAL,
    pressure REAL,
    vibration REAL,
    location TEXT,
    batch_id TEXT,
    timestamp DATETIME,
    source_type TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS api_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id TEXT,
    value REAL,
    unit TEXT,
    status TEXT,
    api_version TEXT,
    response_time REAL,
    city TEXT,
    timestamp DATETIME,
    source_type TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS system_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_level TEXT,
    component TEXT,
    message TEXT,
    user_id TEXT,
    session_id TEXT,
    ip_address TEXT,
    timestamp DATETIME,
    source_type TEXT
)
''')

conn.commit()
conn.close()
print("Nouvelles tables créées")
