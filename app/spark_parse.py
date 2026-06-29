from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = (SparkSession.builder
    .appName("parse-transactions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

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
    .select("data.*"))

query = (parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/checkpoints/parsed")
    .option("path", "/opt/delta/transactions_parsed")
    .trigger(availableNow=True)
    .start())
query.awaitTermination()