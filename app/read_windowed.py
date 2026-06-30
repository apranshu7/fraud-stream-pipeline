from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window

spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/delta/transactions_windowed")
final_df = df.select(col("window.start").alias("start_time"),col("window.end").alias("end_time"),col("user_id"),col("count"))

final_df.printSchema()
print("count:", final_df.count())
final_df.orderBy("start_time", "user_id").show(30, truncate=False)