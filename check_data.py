import sqlite3
import pandas as pd

conn = sqlite3.connect('data/iot_data.db')

# Vérifier les données IoT
print("=== DONNÉES IOT ===")
iot_df = pd.read_sql_query("SELECT * FROM iot_measurements LIMIT 5", conn)
print(iot_df)

print("\n=== DONNÉES CSV ===")
csv_df = pd.read_sql_query("SELECT * FROM csv_data LIMIT 5", conn)
print(csv_df)

print("\n=== LOGS SYSTÈME ===")
logs_df = pd.read_sql_query("SELECT * FROM system_logs LIMIT 5", conn)
print(logs_df)

print("\n=== STATISTIQUES ===")
stats = pd.read_sql_query('''
    SELECT source_type, COUNT(*) as count 
    FROM (
        SELECT 'iot' as source_type FROM iot_measurements
        UNION ALL
        SELECT 'csv' as source_type FROM csv_data
        UNION ALL
        SELECT 'api' as source_type FROM api_data
        UNION ALL
        SELECT 'logs' as source_type FROM system_logs
    ) 
    GROUP BY source_type
''', conn)
print(stats)

conn.close()
