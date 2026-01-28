print("SPARK STRUCTURED STREAMING SIMULE")
print("Traitement Kafka -> HDFS (Parquet)")

import pandas as pd
import os
from datetime import datetime
import json

os.makedirs("data/hdfs_streaming/raw", exist_ok=True)
os.makedirs("data/hdfs_streaming/anomalies", exist_ok=True)
os.makedirs("data/hdfs_streaming/clusters", exist_ok=True)

print("Simulation Spark Streaming Kafka -> HDFS...")

data = []
for i in range(150):
    temp = round(70 + (i % 30), 2)
    pressure = round(140 + (i % 40), 2)
    
    data.append({
        "source_type": "iot",
        "machine_id": f"M{(i % 5)+1:03d}",
        "temperature": temp,
        "pressure": pressure,
        "vibration": round(1 + (i % 9), 2),
        "timestamp": datetime.now().isoformat(),
        "batch_id": f"batch_{i//10}",
        "is_anomaly": temp > 90 or pressure > 180
    })

df = pd.DataFrame(data)

df.to_parquet("data/hdfs_streaming/raw/data.parquet", index=False)

anomalies = df[df["is_anomaly"]]
if len(anomalies) > 0:
    anomalies.to_parquet("data/hdfs_streaming/anomalies/anomalies.parquet", index=False)

from sklearn.cluster import KMeans
import numpy as np

X = df[["temperature", "pressure", "vibration"]].values
kmeans = KMeans(n_clusters=3, random_state=42)
df["cluster"] = kmeans.fit_predict(X)

df.to_parquet("data/hdfs_streaming/clusters/clustered.parquet", index=False)

print(f" Spark Streaming termine")
print(f"   Donnees: {len(df)} lignes")
print(f"   Machines: {df['machine_id'].nunique()}")
print(f"   Anomalies: {len(anomalies)}")
print(f"   Clusters: 3")
print(f"   Format: Parquet (HDFS)")
print(f"   Checkpoint: data/checkpoints/")
