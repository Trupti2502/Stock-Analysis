from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Created Spark session
spark = SparkSession \
    .builder \
    .appName("Battery_charge_company") \
    .getOrCreate()

raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hblpower") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

#Created Schema for Structured Streaming

#timestamp,open,high,low,close,volume

schema = StructType(
    [
            StructField("open", StringType()),
            StructField("high", StringType()),
            StructField("low", StringType()),
            StructField("close", StringType()),
            StructField("volume", StringType()),
            StructField("date", StringType())
    ])
# Applied schema on data
data1= raw_df.select(from_json(raw_df.value, schema).alias("data"))

output_df1 = data1.select(to_json(struct(col("data.close"),col("data.open"),col("data.date"))).alias("value"))

# Sending the data to kafka brocker
query = output_df1.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "/tmp/checkpoint2").option("topic", "hblpower1").start()

# Waits for the termination signal from user
query.awaitTermination()
