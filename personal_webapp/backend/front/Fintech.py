import numpy as np
import pandas as pd
import ccxt
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Binance API credentials
api_key = "qHsgWDMVoKKB9CNacVKPc9giyRyVgm8YdGqAFjJTibzc6FEMKEJdmBXvDlVzrTiK"

# Create a Binance client
binance = ccxt.binance({
    'apiKey': api_key,
    'enableRateLimit': True,
    'rateLimit': 2000  # Adjust the rate limit as needed
})

# Define the Kafka producer function
from confluent_kafka import Producer

kafka_broker = "localhost:9092"
kafka_topic = "binance"

producer = Producer({'bootstrap.servers': kafka_broker})

def send_to_kafka(symbol, closed_price):
    producer.produce(kafka_topic, key=symbol, value=str(closed_price))
    producer.flush()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "binance"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("FATAL")

    # Construct a streaming DataFrame that reads from testtopic
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()
    
    data_Schema = StructType([
    StructField("id", StringType(), True),
    StructField("BTC/USDT", StringType(), True)
])

    
    df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)") \
    .select(from_json(col("value"), data_Schema).alias("data"), col("timestamp")) \
    .select("data.id", "data.BTC/USDT", col("timestamp")) \
    .toDF("Id", "BTC/USDT", "Timestamp")


     # Write final result into console for debugging purpose
    trans_detail_write_stream = df1 \
        .writeStream \
        .outputMode("append") \
        .format("memory")  \
        .queryName("DataFrameKafka") \
        .start() \
        .awaitTermination(200)
  

    spark.stop()

