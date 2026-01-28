import pandas as pd
import json
import os
from datetime import datetime

class HiveSimulator:
    def __init__(self, base_path="data/hive_warehouse"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        
        self.tables = {}
        self.metastore_file = os.path.join(base_path, "metastore.json")
        
        self.load_metastore()
    
    def load_metastore(self):
        if os.path.exists(self.metastore_file):
            with open(self.metastore_file, 'r') as f:
                self.tables = json.load(f)
        else:
            self.tables = {}
    
    def save_metastore(self):
        with open(self.metastore_file, 'w') as f:
            json.dump(self.tables, f, indent=2)
    
    def create_table(self, table_name, schema, location=None):
        if location is None:
            location = os.path.join(self.base_path, table_name)
        
        os.makedirs(location, exist_ok=True)
        
        self.tables[table_name] = {
            "schema": schema,
            "location": location,
            "created_at": datetime.now().isoformat(),
            "row_count": 0
        }
        
        self.save_metastore()
        print(f"Table créée: {table_name}")
    
    def insert_into(self, table_name, data_df):
        if table_name not in self.tables:
            print(f"Table {table_name} n'existe pas")
            return False
        
        table_info = self.tables[table_name]
        location = table_info["location"]
        
        file_path = os.path.join(location, f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        
        data_df.to_parquet(file_path, index=False)
        
        table_info["row_count"] += len(data_df)
        self.save_metastore()
        
        print(f"Données insérées dans {table_name}: {len(data_df)} lignes")
        return True
    
    def select(self, table_name, query=None):
        if table_name not in self.tables:
            print(f"Table {table_name} n'existe pas")
            return None
        
        table_info = self.tables[table_name]
        location = table_info["location"]
        
        if not os.path.exists(location):
            return pd.DataFrame()
        
        all_data = []
        for file in os.listdir(location):
            if file.endswith('.parquet'):
                file_path = os.path.join(location, file)
                df = pd.read_parquet(file_path)
                all_data.append(df)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            
            if query:
                try:
                    result = result.query(query)
                except:
                    print("Erreur dans la requête")
            
            return result
        
        return pd.DataFrame()
    
    def show_tables(self):
        print("Tables dans Hive:")
        for table_name, info in self.tables.items():
            print(f"  {table_name}: {info['row_count']} lignes")

if __name__ == "__main__":
    hive = HiveSimulator()
    
    schema = {
        "columns": [
            {"name": "machine_id", "type": "string"},
            {"name": "temperature", "type": "double"},
            {"name": "pressure", "type": "double"},
            {"name": "vibration", "type": "double"},
            {"name": "timestamp", "type": "timestamp"}
        ]
    }
    
    hive.create_table("iot_measurements", schema)
    
    sample_data = pd.DataFrame({
        "machine_id": ["M001", "M002", "M003"],
        "temperature": [75.5, 82.3, 91.8],
        "pressure": [150.2, 165.7, 178.9],
        "vibration": [3.1, 4.2, 5.6],
        "timestamp": pd.to_datetime(["2024-01-28 10:00:00", "2024-01-28 10:01:00", "2024-01-28 10:02:00"])
    })
    
    hive.insert_into("iot_measurements", sample_data)
    
    hive.show_tables()
    
    result = hive.select("iot_measurements", "temperature > 80")
    print(f"\nRésultats de la requête: {len(result)} lignes")
    print(result)
