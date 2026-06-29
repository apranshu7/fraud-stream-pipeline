from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
df = spark.read.format("delta").load("/opt/delta/transactions_parsed")
df.printSchema()
print("count:", df.count())
df.filter("amount > 4000").show(5, truncate=False)