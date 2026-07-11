from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from datetime import datetime, timezone, timedelta

spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/delta/transactions_running_final")
final_df = df.select(col("window.start").alias("start_time"),col("window.end").alias("end_time"),col("user_id"),col("count"))

# cutoff = (datetime.now(timezone.utc)-timedelta(minutes=4)).isoformat()
# late_detect_df = final_df.filter(col("start_time") >= "2026-07-06 18:30")

final_df.printSchema()
print("count:", final_df.count())
# final_df.orderBy("start_time", "user_id").show(30, truncate=False)
df.groupBy("window.start", "window.end", "user_id").count().withColumnRenamed("count","n").filter("n > 1").show()
# late_detect_df.orderBy("start_time", "user_id").show(30, truncate=False)