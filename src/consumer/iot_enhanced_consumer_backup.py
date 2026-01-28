from kafka import KafkaConsumer
import json
import pandas as pd
import time

class IoTConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            "iot_sensor_data",
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="iot_enhanced_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8"))
        )
        self.data = []
    
    def analyze_batch(self, batch):
        if not batch:
            return
        
        df = pd.DataFrame(batch)
        
        print("\n" + "=" * 40)
        print("BATCH ANALYSE: " + str(len(df)) + " messages")
        print("-" * 40)
        
        print("Machines: " + ", ".join(df["machine_id"].unique()))
        print("Temperature moyenne: " + str(round(df["temperature"].mean(), 1)) + "C")
        print("Pression moyenne: " + str(round(df["pressure"].mean(), 1)) + " PSI")
        
        # Verifier alertes
        high_temp = df[df["temperature"] > 90]
        high_pressure = df[df["pressure"] > 180]
        
        if len(high_temp) > 0:
            print("ALERTE TEMPERATURE: " + str(len(high_temp)) + " mesures > 90C")
        if len(high_pressure) > 0:
            print("ALERTE PRESSION: " + str(len(high_pressure)) + " mesures > 180 PSI")
        
        print("=" * 40)
    
    def run(self, duration_seconds=20):
        print("CONSOMMATEUR IoT - SURVEILLANCE")
        print("Lecture des donnees pendant " + str(duration_seconds) + " secondes")
        print("-" * 40)
        
        start_time = time.time()
        batch = []
        batch_size = 5
        
        try:
            for message in self.consumer:
                batch.append(message.value)
                
                if len(batch) >= batch_size:
                    self.analyze_batch(batch)
                    batch = []
                
                # Arret apres duree
                if time.time() - start_time >= duration_seconds:
                    print("Duree ecoulee - Arret du consommateur")
                    break
                
        except KeyboardInterrupt:
            print("Arret demande")
        finally:
            self.consumer.close()
            
            if batch:
                self.analyze_batch(batch)
            
            print("\nSURVEILLANCE TERMINEE")
            print("Temps total: " + str(round(time.time() - start_time, 1)) + "s")

if __name__ == "__main__":
    consumer = IoTConsumer()
    consumer.run(duration_seconds=15)  # 15 secondes
