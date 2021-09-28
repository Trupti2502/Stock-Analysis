
# To start pyspark shell
# ./pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2

# To Create output topic
# ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic hblpower1

# To run using spark submit
# ./spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 home/ubuntu/Desktop/EV/Battery/hblpower.py

# To get data from terminal using kafka consumer
# ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hblpower1 --from-beginning

# Import Required Libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Created spark session
spark = SparkSession \
    .builder \
    .appName("Battery_charge_company") \
    .getOrCreate()

# Created kafka consumer using spark readStream
raw_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "hblpower") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING)")
# Created Schema for Structured Streaming

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

# Selecting the required data for processing 

output_df = data1.select(to_json(struct(col("data.volume"), col("data.date"))).alias("value"))

# Sending the data to kafka brocker 
query = output_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "/tmp/checkpoint1").option("topic", "hblpower1").start()

# Waits for the termination signal from user
query.awaitTermination()


