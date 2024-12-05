from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min, window

KAFKA_TOPIC = "weather_stream"
MONGO_URI = "mongodb+srv://manju1172004:1234@cluster0.hbfmf.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .config("spark.mongodb.output.uri", MONGO_URI) \
    .getOrCreate()

# Schema Definition
weather_schema = """
    main STRUCT<temp: DOUBLE, humidity: DOUBLE>,
    dt BIGINT
"""

# Read Data from Kafka
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Extract and Process
weather_data = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), weather_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", (col("dt") / 3600).cast("timestamp"))

aggregated_data = weather_data.groupBy(window("timestamp", "1 hour")) \
    .agg(
        avg("main.temp").alias("hour_avg_temp"),
        max("main.temp").alias("max_temp"),
        min("main.temp").alias("min_temp")
    )

# Write to MongoDB
query = aggregated_data.writeStream \
    .format("mongodb") \
    .outputMode("append") \
    .start()

query.awaitTermination()
