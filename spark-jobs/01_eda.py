"""
01_eda.py — EDA với Spark: phân tích phân phối giá, diện tích, vị trí.
Kết quả lưu vào HDFS dạng parquet để Hive đọc.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("RealEstate EDA") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Đọc CSV đã clean
df = spark.read.csv("hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
                    header=True, inferSchema=True)
print(f"Total listings: {df.count()}")
df.printSchema()

# ── 1. Thống kê theo loại BĐS ─────────────────────────────────
by_type = df.groupBy("property_type").agg(
    F.count("*").alias("total_listings"),
    F.round(F.avg("price_billion"), 3).alias("avg_price_billion"),
    F.round(F.avg("area_m2"), 1).alias("avg_area_m2"),
    F.round(F.avg("price_per_m2_million"), 1).alias("avg_price_per_m2"),
    F.round(F.min("price_billion"), 3).alias("min_price"),
    F.round(F.max("price_billion"), 3).alias("max_price"),
).orderBy(F.desc("total_listings"))

by_type.show(truncate=False)
by_type.write.mode("overwrite").parquet("hdfs://namenode:9000/realestate/batch/by_type")

# ── 2. Thống kê theo quận/huyện ──────────────────────────────
by_district = df.groupBy("district", "city").agg(
    F.count("*").alias("total_listings"),
    F.round(F.avg("price_billion"), 3).alias("avg_price_billion"),
    F.round(F.avg("price_per_m2_million"), 1).alias("avg_price_per_m2"),
    F.round(F.percentile_approx("price_billion", 0.5), 3).alias("median_price"),
).orderBy(F.desc("total_listings"))

by_district.show(20, truncate=False)
by_district.write.mode("overwrite").parquet("hdfs://namenode:9000/realestate/batch/by_district")

# ── 3. Phân phối price_tier ───────────────────────────────────
price_dist = df.groupBy("price_tier").agg(
    F.count("*").alias("count"),
    F.round(F.avg("area_m2"), 1).alias("avg_area_m2"),
).orderBy("price_tier")

price_dist.show(truncate=False)
price_dist.write.mode("overwrite").parquet("hdfs://namenode:9000/realestate/batch/price_dist")

# ── 4. Phân tích pháp lý ──────────────────────────────────────
by_legal = df.groupBy("legal_status").agg(
    F.count("*").alias("count"),
    F.round(F.avg("price_billion"), 3).alias("avg_price"),
).orderBy(F.desc("count"))

by_legal.show(truncate=False)
by_legal.write.mode("overwrite").parquet("hdfs://namenode:9000/realestate/batch/by_legal")

# ── 5. Phân tích hướng nhà ────────────────────────────────────
by_direction = df.groupBy("direction").agg(
    F.count("*").alias("count"),
    F.round(F.avg("price_per_m2_million"), 1).alias("avg_price_per_m2"),
).orderBy(F.desc("count"))

by_direction.show(truncate=False)
by_direction.write.mode("overwrite").parquet("hdfs://namenode:9000/realestate/batch/by_direction")

print("EDA hoàn tất. Kết quả đã lưu vào HDFS /realestate/batch/")
spark.stop()
