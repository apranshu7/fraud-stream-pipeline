from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("kafka-to-delta-raw")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
)

query = (
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/checkpoints/raw")
    .start("/opt/delta/transactions_raw")
)

query.awaitTermination()