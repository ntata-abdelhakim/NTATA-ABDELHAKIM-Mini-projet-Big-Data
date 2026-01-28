print("🚀 DEMARRAGE SPARK SIMULE")
print("Cette version simule Spark sans Docker")

import pandas as pd
import os
from datetime import datetime

# 1. Creer structure HDFS
os.makedirs("data/hdfs_spark", exist_ok=True)
os.makedirs("data/hdfs_spark/raw", exist_ok=True)
os.makedirs("data/hdfs_spark/anomalies", exist_ok=True)

# 2. Generer donnees
data = []
for i in range(100):
    data.append({
        "machine_id": f"M{(i % 5)+1:03d}",
        "temperature": 70 + (i % 30),
        "pressure": 140 + (i % 40),
        "vibration": 1 + (i % 9),
        "timestamp": datetime.now().isoformat(),
        "source": "spark_streaming"
    })

df = pd.DataFrame(data)

# 3. Sauvegarder en Parquet (format Big Data)
df.to_parquet("data/hdfs_spark/raw/data.parquet")

# 4. Detector anomalies
anomalies = df[(df["temperature"] > 90) | (df["pressure"] > 180)]
anomalies.to_parquet("data/hdfs_spark/anomalies/anomalies.parquet")

# 5. Statistiques
stats = df.groupby("machine_id").agg({
    "temperature": ["mean", "max"],
    "pressure": ["mean", "max"]
})

print(f"✅ SPARK TERMINE")
print(f"   Donnees: {len(df)} lignes")
print(f"   Anomalies: {len(anomalies)}")
print(f"   Fichiers Parquet crees")
print(f"   Chemin: data/hdfs_spark/")
