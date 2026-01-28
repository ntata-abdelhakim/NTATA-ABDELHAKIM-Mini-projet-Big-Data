from flask import Flask, jsonify
import pandas as pd
from datetime import datetime
import os

app = Flask(__name__)

class IoTAPI:
    def __init__(self):
        self.data_dir = "data/processed"
        self.load_data()
    
    def load_data(self):
        self.sensors_data = []
        try:
            csv_files = [f for f in os.listdir(self.data_dir) if f.endswith('.csv')]
            for file in csv_files[:2]:  # Charger max 2 fichiers
                df = pd.read_csv(os.path.join(self.data_dir, file))
                for _, row in df.iterrows():
                    self.sensors_data.append(row.to_dict())
        except:
            self.create_sample_data()
    
    def create_sample_data(self):
        import numpy as np
        np.random.seed(42)
        for i in range(30):
            self.sensors_data.append({
                "id": i + 1,
                "machine_id": f"M{np.random.randint(1,6):03d}",
                "temperature": round(np.random.uniform(50, 100), 2),
                "pressure": round(np.random.uniform(100, 200), 2),
                "vibration": round(np.random.uniform(0, 10), 2),
                "timestamp": datetime.now().isoformat()
            })
    
    def get_stats(self):
        if not self.sensors_data:
            return {}
        
        df = pd.DataFrame(self.sensors_data)
        return {
            "total_records": len(df),
            "machines": df["machine_id"].unique().tolist() if "machine_id" in df.columns else [],
            "avg_temperature": round(df["temperature"].mean(), 2) if "temperature" in df.columns else 0,
            "avg_pressure": round(df["pressure"].mean(), 2) if "pressure" in df.columns else 0,
            "last_update": datetime.now().isoformat()
        }
    
    def get_alerts(self):
        alerts = []
        for record in self.sensors_data[-20:]:
            if "temperature" in record and record["temperature"] > 90:
                alerts.append({
                    "machine": record.get("machine_id", "unknown"),
                    "metric": "temperature",
                    "value": record["temperature"],
                    "threshold": 90
                })
            if "pressure" in record and record["pressure"] > 180:
                alerts.append({
                    "machine": record.get("machine_id", "unknown"),
                    "metric": "pressure",
                    "value": record["pressure"],
                    "threshold": 180
                })
        return alerts

api = IoTAPI()

@app.route("/")
def home():
    return jsonify({
        "api": "IoT Platform API",
        "version": "1.0",
        "endpoints": [
            {"path": "/", "description": "Documentation"},
            {"path": "/data", "description": "Toutes les donnees"},
            {"path": "/stats", "description": "Statistiques"},
            {"path": "/alerts", "description": "Alertes"},
            {"path": "/machines", "description": "Liste machines"},
            {"path": "/machine/<id>", "description": "Donnees machine"}
        ]
    })

@app.route("/data")
def get_data():
    return jsonify({
        "count": len(api.sensors_data),
        "data": api.sensors_data[-30:]
    })

@app.route("/stats")
def get_stats():
    return jsonify(api.get_stats())

@app.route("/alerts")
def get_alerts():
    alerts = api.get_alerts()
    return jsonify({
        "count": len(alerts),
        "alerts": alerts
    })

@app.route("/machines")
def get_machines():
    stats = api.get_stats()
    return jsonify({
        "machines": stats.get("machines", []),
        "count": len(stats.get("machines", []))
    })

@app.route("/machine/<machine_id>")
def get_machine(machine_id):
    machine_data = [d for d in api.sensors_data if d.get("machine_id") == machine_id]
    if not machine_data:
        return jsonify({"error": "Machine non trouvee"}), 404
    
    df = pd.DataFrame(machine_data)
    return jsonify({
        "machine": machine_id,
        "records": len(machine_data),
        "avg_temperature": round(df["temperature"].mean(), 2) if "temperature" in df.columns else 0,
        "avg_pressure": round(df["pressure"].mean(), 2) if "pressure" in df.columns else 0,
        "recent_data": machine_data[-10:]
    })

if __name__ == "__main__":
    print("="*60)
    print("API IoT Platform - Flask Server")
    print("="*60)
    print("URL: http://localhost:5000")
    print("Endpoints:")
    print("  /          - Documentation")
    print("  /data      - Donnees IoT")
    print("  /stats     - Statistiques")
    print("  /alerts    - Alertes en cours")
    print("  /machines  - Liste machines")
    print("="*60)
    app.run(debug=True, host="0.0.0.0", port=5000)
