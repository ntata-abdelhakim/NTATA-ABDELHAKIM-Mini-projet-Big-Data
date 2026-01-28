import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
import os

print("VISUALISATION DES DONNEES IoT")
print("="*50)

np.random.seed(123)
hours = list(range(24))
machines = ["M001", "M002", "M003"]

data = []
for hour in hours:
    for machine in machines:
        # Variation circadienne
        base_temp = 75 + np.sin(hour/24*2*np.pi)*10
        temp = base_temp + np.random.normal(0, 3)
        pressure = 150 + np.random.normal(0, 10)
        
        data.append({
            "hour": hour,
            "machine": machine,
            "temperature": round(temp, 1),
            "pressure": round(pressure, 1),
            "anomaly": temp > 85 or pressure > 170
        })

df = pd.DataFrame(data)

print("Donnees generees pour visualisation")
print(f"Periodes: {len(hours)} heures")
print(f"Machines: {len(machines)}")
print(f"Total points: {len(df)}")

plt.figure(figsize=(12, 8))

plt.subplot(2, 2, 1)
for machine in machines:
    machine_data = df[df["machine"] == machine]
    plt.plot(machine_data["hour"], machine_data["temperature"], 
             marker="o", label=machine, linewidth=2)
plt.axhline(y=85, color="r", linestyle="--", label="Seuil alerte (85C)")
plt.xlabel("Heure de la journee")
plt.ylabel("Temperature (C)")
plt.title("Evolution temperature - 24 heures")
plt.legend()
plt.grid(True, alpha=0.3)

plt.subplot(2, 2, 2)
for machine in machines:
    machine_data = df[df["machine"] == machine]
    plt.plot(machine_data["hour"], machine_data["pressure"], 
             marker="s", label=machine, linewidth=2)
plt.axhline(y=170, color="r", linestyle="--", label="Seuil alerte (170 PSI)")
plt.xlabel("Heure de la journee")
plt.ylabel("Pression (PSI)")
plt.title("Evolution pression - 24 heures")
plt.legend()
plt.grid(True, alpha=0.3)

plt.subplot(2, 2, 3)
temps = df["temperature"].values
plt.hist(temps, bins=15, edgecolor="black", alpha=0.7)
plt.axvline(x=np.mean(temps), color="r", linestyle="--", 
            label=f"Moyenne: {np.mean(temps):.1f}C")
plt.xlabel("Temperature (C)")
plt.ylabel("Frequence")
plt.title("Distribution des temperatures")
plt.legend()
plt.grid(True, alpha=0.3)

plt.subplot(2, 2, 4)
anomalies_by_machine = df.groupby("machine")["anomaly"].sum()
colors = []
for count in anomalies_by_machine.values:
    if count == 0:
        colors.append("green")
    elif count < 3:
        colors.append("orange")
    else:
        colors.append("red")

bars = plt.bar(anomalies_by_machine.index, anomalies_by_machine.values, 
               color=colors, edgecolor="black")
plt.xlabel("Machine")
plt.ylabel("Nombre d anomalies")
plt.title("Anomalies detectees par machine")
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height + 0.1,
             f"{int(height)}", ha="center", va="bottom")

plt.tight_layout()

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
output_dir = "data/processed/visualizations"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, f"iot_dashboard_{timestamp}.png")
plt.savefig(output_file, dpi=150, bbox_inches="tight")

print(f"\nGraphique sauvegarde: {output_file}")
print("Visualisation terminee avec succes!")

print("\n" + "="*50)
print("RESUME STATISTIQUE:")
print(f"Temperature moyenne: {df['temperature'].mean():.1f}C")
print(f"Temperature max: {df['temperature'].max():.1f}C")
print(f"Temperature min: {df['temperature'].min():.1f}C")
print(f"Pression moyenne: {df['pressure'].mean():.1f} PSI")
print(f"Anomalies totales: {df['anomaly'].sum()}")
print("="*50)

response = input("\nVoulez-vous afficher le graphique? (o/n): ")
if response.lower() == "o":
    plt.show()
else:
    print("Graphique sauvegarde seulement")
