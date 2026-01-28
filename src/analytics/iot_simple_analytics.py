import pandas as pd
import numpy as np
from datetime import datetime

print("ANALYSE DE DONNEES IoT")
print("="*50)

print("1. Creation de donnees de test...")
np.random.seed(42)

# Creer 50 mesures
data = []
for i in range(50):
    machine = "M" + str(np.random.randint(1,6)).zfill(3)
    temp = round(np.random.uniform(50, 100), 2)
    pressure = round(np.random.uniform(100, 200), 2)
    vibration = round(np.random.uniform(0, 10), 2)
    
    data.append({
        "machine_id": machine,
        "temperature": temp,
        "pressure": pressure,
        "vibration": vibration,
        "timestamp": "2024-01-01 " + str(np.random.randint(0,24)).zfill(2) + ":" + str(np.random.randint(0,60)).zfill(2) + ":00"
    })

df = pd.DataFrame(data)
print("   " + str(len(df)) + " mesures creees")

print("\n2. Statistiques generales...")
print("   Machines uniques: " + str(len(df["machine_id"].unique())))
print("   Temperature moyenne: " + str(round(df["temperature"].mean(), 2)) + "C")
print("   Temperature max: " + str(round(df["temperature"].max(), 2)) + "C")
print("   Temperature min: " + str(round(df["temperature"].min(), 2)) + "C")
print("   Pression moyenne: " + str(round(df["pressure"].mean(), 2)) + " PSI")

print("\n3. Analyse par machine...")
for machine in sorted(df["machine_id"].unique()):
    machine_data = df[df["machine_id"] == machine]
    avg_temp = machine_data["temperature"].mean()
    avg_pressure = machine_data["pressure"].mean()
    print("   " + machine + ": " + str(round(avg_temp, 1)) + "C, " + str(round(avg_pressure, 1)) + " PSI")

print("\n4. Detection d anomalies...")
high_temp = df[df["temperature"] > 90]
high_pressure = df[df["pressure"] > 180]

if len(high_temp) > 0:
    print("   ALERTE: " + str(len(high_temp)) + " mesures temperature > 90C")
    for index, row in high_temp.head(2).iterrows():
        print("     Machine " + row["machine_id"] + ": " + str(row["temperature"]) + "C")

if len(high_pressure) > 0:
    print("   ALERTE: " + str(len(high_pressure)) + " mesures pression > 180 PSI")

if len(high_temp) == 0 and len(high_pressure) == 0:
    print("   AUCUNE anomalie detectee")

print("\n5. Sauvegarde...")
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_file = "data/processed/iot_analysis_" + timestamp + ".csv"
df.to_csv(csv_file, index=False)
print("   Donnees sauvegardees: " + csv_file)

print("\n" + "="*50)
print("ANALYSE TERMINEE AVEC SUCCES")
