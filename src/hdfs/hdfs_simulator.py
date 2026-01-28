import os
import json
from datetime import datetime

class HDFSSimulator:
    def __init__(self, base_path="data/hdfs_simulated"):
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        
        self.hierarchy = {
            "iot": {
                "data": ["raw", "processed", "anomalies", "clusters"],
                "models": ["rf_model", "kmeans_model"],
                "logs": ["spark", "hadoop"],
                "warehouse": ["hive"]
            }
        }
        
        self.create_structure()
    
    def create_structure(self):
        for folder, subfolders in self.hierarchy.items():
            for subfolder, items in subfolders.items():
                for item in items:
                    path = os.path.join(self.base_path, folder, subfolder, item)
                    os.makedirs(path, exist_ok=True)
                    print(f"Créé: {path}")
    
    def write_parquet(self, df, path):
        full_path = os.path.join(self.base_path, path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        
        df.to_parquet(full_path + ".parquet", index=False)
        print(f"Écrit: {full_path}.parquet")
    
    def read_parquet(self, path):
        full_path = os.path.join(self.base_path, path + ".parquet")
        if os.path.exists(full_path):
            import pandas as pd
            return pd.read_parquet(full_path)
        return None
    
    def list_files(self, path=""):
        full_path = os.path.join(self.base_path, path)
        if os.path.exists(full_path):
            return os.listdir(full_path)
        return []
    
    def get_stats(self):
        total_size = 0
        file_count = 0
        
        for root, dirs, files in os.walk(self.base_path):
            for file in files:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
                file_count += 1
        
        return {
            "total_size_mb": total_size / (1024 * 1024),
            "file_count": file_count,
            "hierarchy": self.hierarchy
        }

if __name__ == "__main__":
    hdfs = HDFSSimulator()
    print("Structure HDFS simulée créée:")
    print(json.dumps(hdfs.get_stats(), indent=2))
