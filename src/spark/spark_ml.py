from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np

spark = SparkSession.builder \
    .appName("IoT-ML-Analytics") \
    .getOrCreate()

df = spark.read.parquet("hdfs://namenode:9000/iot/data/raw")

if df.count() > 0:
    assembler = VectorAssembler(
        inputCols=["temperature", "pressure", "vibration"],
        outputCol="features"
    )
    
    assembled = assembler.transform(df)
    
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    scaler_model = scaler.fit(assembled)
    scaled_data = scaler_model.transform(assembled)
    
    kmeans = KMeans(k=3, seed=42, featuresCol="scaled_features")
    kmeans_model = kmeans.fit(scaled_data)
    
    clustered = kmeans_model.transform(scaled_data)
    
    from pyspark.sql import Window
    from pyspark.sql.functions import lag
    
    window = Window.partitionBy("machine_id").orderBy("timestamp")
    
    df_with_lag = df.withColumn("prev_temp", lag("temperature", 1).over(window))
    
    regression_data = df_with_lag.filter(col("prev_temp").isNotNull())
    
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="temperature",
        numTrees=100,
        maxDepth=10,
        seed=42
    )
    
    train_data, test_data = regression_data.randomSplit([0.8, 0.2], seed=42)
    
    if train_data.count() > 10:
        rf_model = rf.fit(train_data)
        
        predictions = rf_model.transform(test_data)
        
        evaluator = RegressionEvaluator(
            labelCol="temperature",
            predictionCol="prediction",
            metricName="rmse"
        )
        
        rmse = evaluator.evaluate(predictions)
        
        print(f"RMSE du modèle: {rmse}")
        
        rf_model.write().overwrite().save("hdfs://namenode:9000/iot/models/rf_model")
        kmeans_model.write().overwrite().save("hdfs://namenode:9000/iot/models/kmeans_model")
        
        print("Modèles ML sauvegardés sur HDFS")
    
spark.stop()
