from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

spark = SparkSession.builder \
    .appName("IoT-RealTime-Streaming") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

schema = StructType([
    StructField("source_type", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("timestamp", StringType(), True)
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

streaming_query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

streaming_query.awaitTermination(60)
