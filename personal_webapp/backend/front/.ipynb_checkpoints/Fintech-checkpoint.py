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

# Define the schema for the Kafka data
schema = StructType([
    StructField("value", StringType()),
])

spark = SparkSession.builder.appName("fintech").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2").getOrCreate()
# Define the symbols you want to monitor
symbols = ['BTC/USDT', 'ETH/USDT']
df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "binance") \
        .option("startingOffsets", "earliest") \
        .load()
# Define the schema for the Kafka data
schema = StructType([StructField("value", StringType())])

# Parse JSON data and extract "coin" and "price" columns
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*") \
    .withColumn("coin", split(col("value"), '/')[0]) \
    .withColumn("price", col("value").cast(DoubleType()))
# Select only the "coin" and "price" columns
result_df = parsed_df.select("coin", "price")

# Output to the console (for testing)
query = result_df.writeStream.queryName("zebra").outputMode("append").format("console").start()

for _ in range(2):
    for symbol in symbols:
        ticker = binance.fetch_ticker(symbol)
        closed_price = ticker['last']
        print(f"{symbol}: Closed Price = {closed_price}")
        send_to_kafka(symbol, closed_price)
    time.sleep(60)
# Await termination of the query
query.awaitTermination()
