from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("read-delta-raw")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load("/opt/delta/transactions_raw")

print(f"\nRow count: {df.count()}\n")
df.printSchema()

# value comes back as raw bytes — cast to string so it's readable
df.selectExpr("CAST(value AS STRING) AS value", "topic", "partition", "offset", "timestamp") \
  .show(10, truncate=False)

spark.stop()