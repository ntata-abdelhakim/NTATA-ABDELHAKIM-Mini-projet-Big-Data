import pandas as pd
import numpy as np
from datetime import datetime

class AnomalyDetector:
    def __init__(self):
        print("DETECTEUR D ANOMALIES IoT")
        print("Algorithme Z-score et seuils multiples")
    
    def detect_with_zscore(self, data, threshold=2.5):
        """Détection avec Z-score"""
        if len(data) < 5:
            return []
        
        mean = np.mean(data)
        std = np.std(data)
        
        if std == 0:
            return []
        
        z_scores = [(x - mean) / std for x in data]
        anomalies = [i for i, z in enumerate(z_scores) if abs(z) > threshold]
        return anomalies
    
    def detect_with_thresholds(self, temperatures, pressures, vibrations):
        """Détection avec seuils fixes"""
        anomalies = []
        
        # Seuils configurables
        temp_threshold_high = 90
        temp_threshold_low = 60
        pressure_threshold = 180
        vibration_threshold = 8
        
        for i in range(len(temperatures)):
            alerts = []
            
            if temperatures[i] > temp_threshold_high:
                alerts.append(f"TEMP_HAUTE({temperatures[i]}C)")
            elif temperatures[i] < temp_threshold_low:
                alerts.append(f"TEMP_BASSE({temperatures[i]}C)")
            
            if pressures[i] > pressure_threshold:
                alerts.append(f"PRESSION({pressures[i]}PSI)")
            
            if vibrations[i] > vibration_threshold:
                alerts.append(f"VIBRATION({vibrations[i]}mm/s)")
            
            if alerts:
                anomalies.append({
                    "index": i,
                    "alerts": alerts,
                    "temperature": temperatures[i],
                    "pressure": pressures[i],
                    "vibration": vibrations[i]
                })
        
        return anomalies
    
    def analyze_dataset(self, df):
        """Analyse complète d'un dataset"""
        print("\n" + "="*50)
        print("ANALYSE COMPLETE DES DONNEES")
        print("="*50)
        
        results = {
            "total_points": len(df),
            "machines": df["machine_id"].unique().tolist(),
            "anomalies_by_type": {},
            "machine_anomalies": {}
        }
        
        # Par machine
        for machine in df["machine_id"].unique():
            machine_data = df[df["machine_id"] == machine]
            
            if len(machine_data) < 3:
                continue
            
            temps = machine_data["temperature"].values
            pressures = machine_data["pressure"].values
            vibrations = machine_data["vibration"].values
            
            # Détection seuils
            threshold_anomalies = self.detect_with_thresholds(temps, pressures, vibrations)
            
            # Détection Z-score (si assez de données)
            if len(temps) >= 10:
                z_anomalies = self.detect_with_zscore(temps, threshold=2.0)
                if z_anomalies:
                    print(f"  {machine}: {len(z_anomalies)} anomalies Z-score detectees")
            
            if threshold_anomalies:
                results["machine_anomalies"][machine] = len(threshold_anomalies)
                
                # Compter par type
                for anomaly in threshold_anomalies:
                    for alert in anomaly["alerts"]:
                        alert_type = alert.split("(")[0]
                        results["anomalies_by_type"][alert_type] = results["anomalies_by_type"].get(alert_type, 0) + 1
        
        # Affichage résultats
        print(f"\nDonnees analysees: {results['total_points']} points")
        print(f"Machines: {', '.join(results['machines'])}")
        
        if results["anomalies_by_type"]:
            print("\nANOMALIES DETECTEES:")
            for alert_type, count in results["anomalies_by_type"].items():
                print(f"  {alert_type}: {count} occurrences")
            
            print("\nPAR MACHINE:")
            for machine, count in results["machine_anomalies"].items():
                print(f"  {machine}: {count} anomalies")
        else:
            print("\nAUCUNE ANOMALIE DETECTEE - Systeme stable")
        
        print("\n" + "="*50)
        
        return results

# Test du detecteur
if __name__ == "__main__":
    # Creer donnees de test
    np.random.seed(42)
    data = []
    for i in range(30):
        data.append({
            "machine_id": f"M{np.random.randint(1,4):03d}",
            "temperature": np.random.uniform(50, 110),  # Certains > 90
            "pressure": np.random.uniform(100, 220),    # Certains > 180
            "vibration": np.random.uniform(0, 12),      # Certains > 8
            "timestamp": f"10:{i:02d}:00"
        })
    
    df = pd.DataFrame(data)
    
    detector = AnomalyDetector()
    results = detector.analyze_dataset(df)
    
    # Sauvegarder rapport
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = f"data/processed/anomaly_report_{timestamp}.json"
    
    import json
    with open(report_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nRapport sauvegarde: {report_file}")
    print("DETECTEUR PRET POUR USAGE REEL")
