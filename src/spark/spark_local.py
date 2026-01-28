from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
import json
import os

spark = SparkSession.builder \
    .appName("IoT-Spark-Local") \
    .config("spark.sql.warehouse.dir", "data/spark_warehouse") \
    .config("spark.sql.streaming.checkpointLocation", "data/checkpoints") \
    .getOrCreate()

os.makedirs("data/spark_warehouse", exist_ok=True)
os.makedirs("data/checkpoints", exist_ok=True)
os.makedirs("data/hdfs_simulated", exist_ok=True)

schema = StructType([
    StructField("source_type", StringType()),
    StructField("machine_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("timestamp", StringType())
])

def read_from_kafka():
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "iot-topic") \
            .option("startingOffsets", "latest") \
            .load()
        
        return df
    except Exception as e:
        print(f"Erreur Kafka: {e}")
        print("Création de données simulées...")
        
        data = [
            ("iot", "M001", 75.5, 150.2, 3.1, "2024-01-28T10:00:00"),
            ("iot", "M002", 82.3, 165.7, 4.2, "2024-01-28T10:01:00"),
            ("iot", "M003", 91.8, 178.9, 5.6, "2024-01-28T10:02:00"),
            ("csv", "M001", 88.4, 172.3, 4.8, "2024-01-28T10:03:00"),
            ("csv", "M002", 79.2, 155.6, 3.9, "2024-01-28T10:04:00")
        ]
        
        return spark.createDataFrame(data, schema)

df = read_from_kafka()

json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def process_batch(df, epoch_id):
    if df.count() > 0:
        df.persist()
        
        df.write \
            .mode("append") \
            .format("parquet") \
            .save("data/hdfs_simulated/iot/data/raw")
        
        anomalies = df.filter((col("temperature") > 90) | (col("pressure") > 180))
        
        if anomalies.count() > 0:
            anomalies.write \
                .mode("append") \
                .format("parquet") \
                .save("data/hdfs_simulated/iot/data/anomalies")
        
        assembler = VectorAssembler(
            inputCols=["temperature", "pressure", "vibration"],
            outputCol="features"
        )
        
        feature_df = assembler.transform(df)
        
        kmeans = KMeans(k=3, seed=42)
        
        if feature_df.count() > 10:
            model = kmeans.fit(feature_df)
            clustered = model.transform(feature_df)
            
            clustered.select("machine_id", "temperature", "pressure", "prediction") \
                .write \
                .mode("append") \
                .format("parquet") \
                .save("data/hdfs_simulated/iot/data/clusters")
            
            model.save("data/hdfs_simulated/iot/models/kmeans_model")
        
        stats = df.groupBy("machine_id").agg(
            avg("temperature").alias("avg_temp"),
            avg("pressure").alias("avg_pressure"),
            count("*").alias("count")
        )
        
        stats.write \
            .mode("overwrite") \
            .format("parquet") \
            .save("data/hdfs_simulated/iot/data/stats")
        
        df.unpersist()
        
        print(f"Batch {epoch_id} traité: {df.count()} enregistrements")

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
