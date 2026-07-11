from pyspark.sql import SparkSession
import json
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.streaming import StreamingQueryListener

spark = (SparkSession.builder
    .appName("window-transactions")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

class MyListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        print("Started")

    def onQueryProgress(self, event):
        data_progress = json.loads(event.progress.json)
        et = data_progress.get("eventTime", {})
        print("The watermark is : ", et.get("watermark"))
        print("The max is : ", et.get("max"))
        if data_progress["stateOperators"]:
            print("Dropped: ", data_progress["stateOperators"][0]["numRowsDroppedByWatermark"])

    def onQueryTerminated(self, event):
        print("Stopped")

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

spark.streams.addListener(MyListener())

query = (windowed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/opt/checkpoints/running_final_checkpoint")
    .option("path", "/opt/delta/transactions_running_final")
    .trigger(processingTime = "30 seconds")
    .start())

query.awaitTermination()