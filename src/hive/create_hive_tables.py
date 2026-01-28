print("HIVE METASTORE SIMULATION")
print("SQL on Big Data (HDFS Parquet)")

import pandas as pd
import json
import os

os.makedirs("data/hive_warehouse", exist_ok=True)

hive_tables = {
    "iot_measurements": {
        "location": "data/hdfs_streaming/raw/data.parquet",
        "format": "parquet",
        "columns": [
            {"name": "source_type", "type": "string"},
            {"name": "machine_id", "type": "string"},
            {"name": "temperature", "type": "double"},
            {"name": "pressure", "type": "double"},
            {"name": "vibration", "type": "double"},
            {"name": "timestamp", "type": "timestamp"},
            {"name": "batch_id", "type": "string"}
        ]
    },
    "iot_anomalies": {
        "location": "data/hdfs_streaming/anomalies/anomalies.parquet",
        "format": "parquet",
        "columns": [
            {"name": "machine_id", "type": "string"},
            {"name": "temperature", "type": "double"},
            {"name": "pressure", "type": "double"},
            {"name": "anomaly_type", "type": "string"}
        ]
    },
    "iot_clusters": {
        "location": "data/hdfs_streaming/clusters/clustered.parquet",
        "format": "parquet",
        "columns": [
            {"name": "machine_id", "type": "string"},
            {"name": "temperature", "type": "double"},
            {"name": "pressure", "type": "double"},
            {"name": "cluster", "type": "int"}
        ]
    }
}

with open("data/hive_warehouse/metastore.json", "w") as f:
    json.dump(hive_tables, f, indent=2)

print(" Hive Metastore cree")
print("Tables disponibles:")

for table_name, table_info in hive_tables.items():
    if os.path.exists(table_info["location"]):
        try:
            df = pd.read_parquet(table_info["location"])
            print(f"  - {table_name}: {len(df)} lignes")
        except:
            print(f"  - {table_name}: fichier existe")
    else:
        print(f"  - {table_name}: a creer")

print("\nExemple requetes Hive SQL:")
print("1. SELECT machine_id, AVG(temperature) FROM iot_measurements GROUP BY machine_id")
print("2. SELECT * FROM iot_anomalies WHERE temperature > 90")
print("3. SELECT cluster, COUNT(*) FROM iot_clusters GROUP BY cluster")
