# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/ubuntu/Desktop/usecase2.py

# Import Required Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Created spark session
spark = SparkSession \
    .builder \
    .appName("usecase2") \
    .getOrCreate()

# Created kafka consumer using spark readStream
raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "project3") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Created Schema for Structured Streaming

schema = StructType(
    [
            StructField("ticker", StringType()),
            StructField("quoteTimestamp", StringType()),
            StructField("bidPrice", FloatType()),
            StructField("bidSize", FloatType()),
            StructField("askPrice", FloatType()),
            StructField("askSize", FloatType()),
            StructField("midPrice", FloatType())
    ])

# Applied schema on data
schema_df = raw_df.select(from_json(raw_df.value, schema).alias("data"))


output_df = schema_df.select(to_json(struct(col("data.quoteTimestamp"),col("data.askPrice"))).alias("value"))
# Sending the data to kafka brocker 
query = output_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "/tmp/checkpoint1").option("topic", "askPriceOutput").start()

# Waits for the termination signal from user.outputMode("complete")
query.awaitTermination()
