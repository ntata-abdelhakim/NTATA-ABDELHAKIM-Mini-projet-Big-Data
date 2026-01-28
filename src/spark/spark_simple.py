from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

print("Demarrage Spark...")

spark = SparkSession.builder \
    .appName("IoT-Spark") \
    .getOrCreate()

schema = StructType([
    StructField("source_type", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "iot-topic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    print("Connecte a Kafka")
    
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination(30)
    
except Exception as e:
    print(f"Erreur Kafka: {e}")
    print("Creation donnees test...")
    
    test_data = [
        ("iot", "M001", 75.5, 150.2, 3.1, "2024-01-28T10:00:00"),
        ("iot", "M002", 82.3, 165.7, 4.2, "2024-01-28T10:01:00"),
        ("iot", "M003", 91.8, 178.9, 5.6, "2024-01-28T10:02:00")
    ]
    
    test_df = spark.createDataFrame(test_data, schema)
    
    test_df.write \
        .mode("overwrite") \
        .parquet("data/hdfs_simulated/test.parquet")
    
    print("Donnees test ecrites")
    
spark.stop()
