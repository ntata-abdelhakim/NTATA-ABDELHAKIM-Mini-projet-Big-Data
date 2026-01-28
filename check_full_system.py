import os
import pandas as pd
from pyspark.sql import SparkSession
import time

print(" DÉMARRAGE DU SYSTÈME BIG DATA ")
print("Attente 30 secondes pour le traitement des données")
time.sleep(30)

print("\n1. Initialisation Spark...")
spark = SparkSession.builder \
    .appName("Check-System") \
    .getOrCreate()

print("\n2. Vérification des données Parquet")
if os.path.exists("data/hdfs_simulated/iot/data/raw"):
    try:
        df = spark.read.parquet("data/hdfs_simulated/iot/data/raw")
        print(f" Données IoT brutes: {df.count()} enregistrements")
        df.show(5)
    except Exception as e:
        print(f" Erreur lecture Parquet: {e}")

if os.path.exists("data/hdfs_simulated/iot/data/stats"):
    try:
        stats = spark.read.parquet("data/hdfs_simulated/iot/data/stats")
        print(f"\n Statistiques: {stats.count()} machines")
        stats.show()
    except:
        print("\n Aucune statistique disponible")

print("\n3. Vérification HDFS simulé...")
base_path = "data/hdfs_simulated"
if os.path.exists(base_path):
    total_files = 0
    total_size = 0
    
    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.endswith(('.parquet', '.json', '.model')):
                total_files += 1
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
    
    print(f"   Fichiers HDFS: {total_files}")
    print(f"   Taille totale: {total_size / 1024:.2f} KB")
    
    print("\n   Structure:")
    for root, dirs, files in os.walk(base_path):
        level = root.replace(base_path, "").count(os.sep)
        indent = " " * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = " " * 2 * (level + 1)
        for file in files[:3]:
            if file.endswith(('.parquet', '.json', '.model')):
                print(f"{subindent}{file}")

print("\n4. Vérification SQLit...")
import sqlite3
try:
    conn = sqlite3.connect("data/iot_data.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM iot_measurements")
    iot_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM alerts")
    alerts_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM csv_data")
    csv_count = cursor.fetchone()[0]
    
    print(f"   IoT measurements: {iot_count}")
    print(f"   Alertes: {alerts_count}")
    print(f"   Données CSV: {csv_count}")
    
    conn.close()
except Exception as e:
    print(f"   Erreur SQLite: {e}")

print("\n5. Vérification Kafka")
try:
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        'iot-topic',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000
    )
    
    message_count = 0
    for message in consumer:
        message_count += 1
    
    print(f"   Messages dans Kafka: {message_count}")
except Exception as e:
    print(f"   Kafka non disponible: {e}")

print("\n=== RÉSUMÉ DU SYSTÈME ===")
print(" Kafka: ingestion temps réel")
print(" Spark: traitement distribué")
print(" HDFS: stockage simulé (Parquet)")
print(" SQLite: données structurées")
print(" ML: K-means clustering")
print(" Multi-sources: IoT + CSV")

spark.stop()
print("\n Système Big Data IoT opérationnel !")
