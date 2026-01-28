import os
import pandas as pd
from pyspark.sql import SparkSession

print("VERIFIER CE SUSTEM ")

print("\n1. Structure HDFS simulée:")
if os.path.exists("data/hdfs_simulated"):
    for root, dirs, files in os.walk("data/hdfs_simulated"):
        level = root.replace("data/hdfs_simulated", "").count(os.sep)
        indent = " " * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = " " * 4 * (level + 1)
        for file in files:
            print(f"{subindent}{file}")

print("\n2. Données Parquet générées:")
parquet_files = []
for root, dirs, files in os.walk("data/hdfs_simulated"):
    for file in files:
        if file.endswith(".parquet"):
            parquet_files.append(os.path.join(root, file))

if parquet_files:
    for file in parquet_files[:5]:
        try:
            df = pd.read_parquet(file)
            print(f"{file}: {len(df)} lignes")
        except:
            print(f"{file}: format Spark")
else:
    print("Aucun fichier Parquet trouvé")

print("\n3. Structure complète:")
print(" Kafka: localhost:9092")
print(" Spark: Structured Streaming")
print(" HDFS: simulé dans data/hdfs_simulated")
print(" Hive: simulé dans data/hive_warehouse")
print(" ML: K-means clustering")
print(" Format: Parquet")

if os.path.exists("data/iot_data.db"):
    import sqlite3
    conn = sqlite3.connect("data/iot_data.db")
    count = pd.read_sql_query("SELECT COUNT(*) as count FROM iot_measurements", conn)['count'][0]
    print(f" SQLite: {count} enregistrements IoT")
    conn.close()
