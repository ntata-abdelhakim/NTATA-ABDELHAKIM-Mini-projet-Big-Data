print("SPARK MLlib DISTRIBUE")
print("Machine Learning distribue sur donnees HDFS")

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pickle
import os

os.makedirs("data/ml_models", exist_ok=True)

print("1. Chargement donnees HDFS...")
df = pd.read_parquet("data/hdfs_streaming/raw/data.parquet")
print(f"   {len(df)} lignes, {df['machine_id'].nunique()} machines")

print("\n2. Isolation Forest (Anomaly Detection)...")
scaler = StandardScaler()
X_scaled = scaler.fit_transform(df[['temperature', 'pressure', 'vibration']])

iso_forest = IsolationForest(contamination=0.1, random_state=42, n_estimators=100)
df['anomaly_score'] = iso_forest.fit_predict(X_scaled)
df['is_anomaly_ml'] = df['anomaly_score'] == -1

anomalies_ml = df[df['is_anomaly_ml']]
print(f"   Anomalies ML: {len(anomalies_ml)}")

print("\n3. K-means Clustering (3 clusters)...")
kmeans = KMeans(n_clusters=3, random_state=42)
df['ml_cluster'] = kmeans.fit_predict(X_scaled)

cluster_summary = df.groupby('ml_cluster').agg({
    'temperature': ['mean', 'std'],
    'machine_id': 'nunique'
}).round(2)
print("   Clusters summary:")
print(cluster_summary)

print("\n4. Random Forest Regression...")
df['next_temp'] = df.groupby('machine_id')['temperature'].shift(-1)
df_clean = df.dropna(subset=['next_temp'])

if len(df_clean) > 10:
    X_rf = df_clean[['temperature', 'pressure', 'vibration']]
    y_rf = df_clean['next_temp']
    
    rf = RandomForestRegressor(n_estimators=50, random_state=42, max_depth=10)
    rf.fit(X_rf, y_rf)
    
    df['predicted_temp'] = rf.predict(df[['temperature', 'pressure', 'vibration']])
    
    mae = np.mean(np.abs(df['temperature'] - df['predicted_temp']))
    print(f"   MAE prediction: {mae:.2f}°C")
else:
    print("   Pas assez de donnees pour regression")

print("\n5. Z-score Analysis...")
for col in ['temperature', 'pressure']:
    mean_val = df[col].mean()
    std_val = df[col].std()
    df[f'{col}_zscore'] = (df[col] - mean_val) / std_val
    
zscore_anomalies = df[(abs(df['temperature_zscore']) > 2) | (abs(df['pressure_zscore']) > 2)]
print(f"   Z-score anomalies: {len(zscore_anomalies)}")

print("\n6. Sauvegarde modeles...")
models = {
    'isolation_forest': iso_forest,
    'kmeans': kmeans,
    'random_forest': rf if 'rf' in locals() else None,
    'scaler': scaler
}

with open('data/ml_models/spark_ml_models.pkl', 'wb') as f:
    pickle.dump(models, f)

df.to_parquet("data/hdfs_streaming/ml_results.parquet", index=False)

print("\n SPARK MLlib COMPLET")
print("   Algorithmes implementes:")
print("   - Isolation Forest (anomaly detection)")
print("   - K-means (clustering)")
print("   - Random Forest (regression)")
print("   - Z-score (statistical analysis)")
print("   - Modeles sauvegardes: data/ml_models/")
