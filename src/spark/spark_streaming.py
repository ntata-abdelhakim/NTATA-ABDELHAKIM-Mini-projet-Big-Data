from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
import json

spark = SparkSession.builder \
    .appName("IoT-BigData-Streaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

schema = StructType([
    StructField("source_type", StringType()),
    StructField("machine_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("timestamp", TimestampType())
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "iot-topic") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def process_batch(df, epoch_id):
    if df.count() > 0:
        df.persist()
        
        df.write \
            .mode("append") \
            .format("parquet") \
            .save("hdfs://namenode:9000/iot/data/raw")
        
        anomalies = df.filter((col("temperature") > 90) | (col("pressure") > 180))
        
        anomalies.write \
            .mode("append") \
            .format("parquet") \
            .save("hdfs://namenode:9000/iot/data/anomalies")
        
        assembler = VectorAssembler(
            inputCols=["temperature", "pressure", "vibration"],
            outputCol="features"
        )
        
        feature_df = assembler.transform(df)
        
        kmeans = KMeans(k=3, seed=42)
        model = kmeans.fit(feature_df)
        
        clustered = model.transform(feature_df)
        
        clustered.select("machine_id", "temperature", "pressure", "prediction") \
            .write \
            .mode("append") \
            .format("parquet") \
            .save("hdfs://namenode:9000/iot/data/clusters")
        
        df.unpersist()
        
        print(f"Batch {epoch_id} traité: {df.count()} enregistrements")

query = json_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

query.awaitTermination()
