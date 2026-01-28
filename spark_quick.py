print(" CREATION SPARK/HDFS SIMULE")
import pandas as pd
import os

# Créer dossier HDFS
os.makedirs("data/hdfs_spark/raw", exist_ok=True)

data = []
for i in range(100):
    data.append({
        "machine_id": f"M{(i % 5)+1:03d}",
        "temperature": 70 + (i % 30),
        "pressure": 140 + (i % 40),
        "vibration": 1 + (i % 9),
        "timestamp": pd.Timestamp.now()
    })

df = pd.DataFrame(data)

# Sauvegarder en Parquet
df.to_parquet("data/hdfs_spark/raw/data.parquet", index=False)

print(f" HDFS créé: data/hdfs_spark/")
print(f" Données: {len(df)} lignes")
print(f" Format: Parquet")
