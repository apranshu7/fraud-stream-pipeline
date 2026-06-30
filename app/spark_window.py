from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (SparkSession.builder
    .appName("window-transactions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("transaction_id", StringType(),  True),
    StructField("user_id",        StringType(),  True),
    StructField("amount",         DoubleType(),  True),
    StructField("currency",       StringType(),  True),
    StructField("merchant",       StringType(),  True),
    StructField("event_time",     StringType(),  True),
])

kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load())

parsed_df = (kafka_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select(col("data.transaction_id"),
    col("data.user_id"),
    col("data.amount"),
    col("data.currency"),
    col("data.merchant"),
    to_timestamp(col("data.event_time")).alias("event_time"))
    )

windowed_df = (parsed_df.withWatermark("event_time","2 minutes").groupBy(window(col("event_time"),"1 minute"),col("user_id")).count())

# query = (windowed_df.writeStream
#     .format("delta")
#     .outputMode("update")
#     .option("checkpointLocation", "/opt/checkpoints/windowed_update")
#     .option("path", "/opt/delta/transactions_windowed_update")
#     .trigger(availableNow=True)
#     .start())
query = (windowed_df.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate",False)
    .trigger(availableNow=True)
    .start())
query.awaitTermination()