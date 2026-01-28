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
