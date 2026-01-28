from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

class IoTProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8")
        )
        self.machines = ["M001", "M002", "M003", "M004", "M005"]
    
    def generate_data(self, machine_id):
        temperature = round(random.uniform(50, 100), 2)
        pressure = round(random.uniform(100, 200), 2)
        vibration = round(random.uniform(0, 10), 2)
        
        return {
            "machine_id": machine_id,
            "temperature": temperature,
            "pressure": pressure,
            "vibration": vibration,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        }
    
    def run(self, duration_seconds=15):
        print("PRODUCTEUR IoT - SIMULATION")
        print("Envoi de donnees pendant " + str(duration_seconds) + " secondes")
        print("Machines: " + ", ".join(self.machines))
        print("-" * 40)
        
        start_time = time.time()
        message_count = 0
        
        try:
            while time.time() - start_time < duration_seconds:
                for machine in self.machines[:3]:  # 3 machines seulement
                    data = self.generate_data(machine)
                    self.producer.send("iot_sensor_data", data)
                    message_count += 1
                    
                    # Afficher seulement certaines mesures
                    if message_count % 3 == 0:
                        print("Message " + str(message_count) + ": " + machine + " - " + str(data["temperature"]) + "C")
                    
                    time.sleep(0.3)
                
                print("--- Cycle termine ---")
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("Arret demande par utilisateur")
        finally:
            self.producer.flush()
            self.producer.close()
            print("\nRESUME:")
            print("Messages envoyes: " + str(message_count))
            print("Temps: " + str(round(time.time() - start_time, 1)) + " secondes")
            print("Producteur arrete")

if __name__ == "__main__":
    producer = IoTProducer()
    producer.run(duration_seconds=12)  # 12 secondes seulement

def generate_csv_simulated_data():
    import random
    from datetime import datetime
    machines = ["M001", "M002", "M003", "M004", "M005"]
    locations = ["Factory_A", "Factory_B", "Warehouse_C", "Plant_D"]
    
    message = {
        "source_type": "csv_simulated",
        "machine_id": random.choice(machines),
        "temperature": round(random.uniform(70, 95), 1),
        "pressure": round(random.uniform(140, 190), 1),
        "vibration": round(random.uniform(1, 8), 1),
        "location": random.choice(locations),
        "batch_id": f"batch_{random.randint(1000, 9999)}",
        "timestamp": datetime.now().isoformat()
    }
    
    producer.send(topic, message)
    print(f"CSV simulated data: {message}")

def generate_api_simulated_data():
    import random
    from datetime import datetime
    sensors = ["temp_sensor_1", "pressure_sensor_2", "vibration_sensor_3"]
    statuses = ["active", "calibrating", "maintenance", "error"]
    
    message = {
        "source_type": "api_simulated",
        "sensor_id": random.choice(sensors),
        "value": round(random.uniform(0, 100), 2),
        "unit": random.choice(["Celsius", "PSI", "mm/s"]),
        "status": random.choice(statuses),
        "api_version": "v2.1",
        "response_time": round(random.uniform(10, 500), 2),
        "timestamp": datetime.now().isoformat()
    }
    
    producer.send(topic, message)
    print(f"API simulated data: {message}")

def start_multi_source_production():
    import random
    import time
    while True:
        source_type = random.choice(["iot", "csv_simulated", "api_simulated"])
        
        if source_type == "iot":
            generate_iot_data()
        elif source_type == "csv_simulated":
            generate_csv_simulated_data()
        else:
            generate_api_simulated_data()
        
        time.sleep(random.uniform(1, 3))

if __name__ == "__main__":
    start_multi_source_production()
