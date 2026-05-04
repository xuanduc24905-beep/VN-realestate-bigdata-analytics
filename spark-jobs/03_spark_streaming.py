"""
03_spark_streaming.py — Spark Structured Streaming đọc từ Kafka 'realestate-stream'.
Xử lý real-time: phân tier giá, tính price/m², ghi vào HDFS mỗi 2 giây.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("RealEstate Kafka Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "12g") \
    .config("spark.executor.cores", "8") \
    .config("spark.sql.shuffle.partitions", "32") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("listing_id",           StringType(),  True),
    StructField("title",                StringType(),  True),
    StructField("property_type",        StringType(),  True),
    StructField("price_raw",            StringType(),  True),
    StructField("price_billion",        DoubleType(),  True),
    StructField("area_m2",              DoubleType(),  True),
    StructField("price_per_m2_million", DoubleType(),  True),
    StructField("city",                 StringType(),  True),
    StructField("district",             StringType(),  True),
    StructField("ward",                 StringType(),  True),
    StructField("address",              StringType(),  True),
    StructField("bedrooms",             IntegerType(), True),
    StructField("bathrooms",            IntegerType(), True),
    StructField("floors",               IntegerType(), True),
    StructField("direction",            StringType(),  True),
    StructField("legal_status",         StringType(),  True),
    StructField("posted_date",          StringType(),  True),
    StructField("contact",              StringType(),  True),
    StructField("url",                  StringType(),  True),
    StructField("scraped_at",           StringType(),  True),
    StructField("price_tier",           StringType(),  True),
    StructField("area_tier",            StringType(),  True),
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "realestate-stream") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

df_parsed = df_kafka.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data"),
    F.col("timestamp").alias("kafka_timestamp"),
).select("data.*", "kafka_timestamp")

# Enrich streaming data
df_processed = df_parsed \
    .withColumn("price_tier",
        F.when(F.col("price_billion") < 2,   "Dưới 2 tỷ")
         .when(F.col("price_billion") < 5,   "2-5 tỷ")
         .when(F.col("price_billion") < 10,  "5-10 tỷ")
         .when(F.col("price_billion") < 20,  "10-20 tỷ")
         .otherwise("Trên 20 tỷ")
    ) \
    .withColumn("area_tier",
        F.when(F.col("area_m2") < 40,  "Dưới 40m²")
         .when(F.col("area_m2") < 80,  "40-80m²")
         .when(F.col("area_m2") < 150, "80-150m²")
         .when(F.col("area_m2") < 300, "150-300m²")
         .otherwise("Trên 300m²")
    ) \
    .withColumn("price_per_m2_million",
        F.when(
            F.col("price_per_m2_million").isNull() & F.col("area_m2").isNotNull() & (F.col("area_m2") > 0),
            F.round(F.col("price_billion") * 1000 / F.col("area_m2"), 2)
        ).otherwise(F.col("price_per_m2_million"))
    ) \
    .withColumn("ingestion_time", F.current_timestamp()) \
    .dropna(subset=["listing_id", "price_billion"])

# Ghi toàn bộ record vào HDFS (speed layer)
query_hdfs = df_processed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/realestate/streaming/listings") \
    .option("checkpointLocation", "hdfs://namenode:9000/realestate/streaming/checkpoint") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

# Aggregation console: top quận theo số tin mới
query_console = df_processed \
    .groupBy("district", "property_type") \
    .agg(
        F.count("*").alias("new_listings"),
        F.round(F.avg("price_billion"), 3).alias("avg_price"),
        F.round(F.avg("price_per_m2_million"), 1).alias("avg_price_m2"),
    ) \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .start()

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streaming...")
finally:
    query_hdfs.stop()
    query_console.stop()
    spark.stop()
    print("Streaming stopped")
