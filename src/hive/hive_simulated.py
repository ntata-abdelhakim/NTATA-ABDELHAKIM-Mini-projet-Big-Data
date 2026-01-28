import pandas as pd
import os

os.makedirs("data/hive_warehouse/iot", exist_ok=True)

print("Création table Hive simulée")

df = pd.DataFrame({
    "machine_id": [f"M{i:03d}" for i in range(1, 6)],
    "avg_temperature": [75.5, 82.3, 79.8, 88.2, 91.5],
    "avg_pressure": [150.2, 165.7, 158.9, 172.3, 185.7],
    "total_records": [100, 85, 92, 78, 110],
    "last_update": pd.Timestamp.now()
})

df.to_parquet("data/hive_warehouse/iot/iot_stats.parquet", index=False)

print("Table Hive créée:")
print(df)

print("\nRequête Hive simulée:")
result = df[df["avg_temperature"] > 80]
print(result)
