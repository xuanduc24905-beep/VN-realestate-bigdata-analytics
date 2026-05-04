"""
05_export.py — Export Serving Layer ra /data/*.parquet cho Streamlit.

Thứ tự:
  1. Export batch views từ HDFS
  2. Tính speed views từ streaming HDFS records
  3. Merge batch + speed → merged views (Serving Layer thực sự)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("RealEstate Export") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ══════════════════════════════════════════════════════════════
# 1. BATCH VIEWS — export từ HDFS về /data/
# ══════════════════════════════════════════════════════════════
BATCH_EXPORTS = {
    "hdfs://namenode:9000/realestate/batch/by_district":   "/data/by_district.parquet",
    "hdfs://namenode:9000/realestate/batch/by_type":       "/data/by_type.parquet",
    "hdfs://namenode:9000/realestate/batch/price_dist":    "/data/price_dist.parquet",
    "hdfs://namenode:9000/realestate/batch/cluster_stats": "/data/cluster_stats.parquet",
    "hdfs://namenode:9000/realestate/batch/clustered":     "/data/clustered.parquet",
    "hdfs://namenode:9000/realestate/batch/by_legal":      "/data/by_legal.parquet",
    "hdfs://namenode:9000/realestate/batch/by_direction":  "/data/by_direction.parquet",
    # Linear Regression
    "hdfs://namenode:9000/realestate/batch/lr_metrics":      "/data/lr_metrics.parquet",
    "hdfs://namenode:9000/realestate/batch/lr_coefficients": "/data/lr_coefficients.parquet",
    "hdfs://namenode:9000/realestate/batch/lr_predictions":  "/data/lr_predictions.parquet",
    "hdfs://namenode:9000/realestate/batch/lr_category_map": "/data/lr_category_map.parquet",
    "hdfs://namenode:9000/realestate/batch/lr_model_params": "/data/lr_model_params.parquet",
}

df_cleaned = spark.read.csv(
    "hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
    header=True, inferSchema=True
)
batch_total = df_cleaned.count()
df_cleaned.write.mode("overwrite").parquet("/data/cleaned.parquet")
print(f"Batch cleaned: {batch_total:,} rows → /data/cleaned.parquet")

for hdfs_path, local_path in BATCH_EXPORTS.items():
    try:
        df = spark.read.parquet(hdfs_path)
        df.write.mode("overwrite").parquet(local_path)
        print(f"[BATCH] {hdfs_path.split('/')[-1]}: {df.count()} rows → {local_path}")
    except Exception as e:
        print(f"[WARN]  Bỏ qua {hdfs_path.split('/')[-1]}: {e}")

# ══════════════════════════════════════════════════════════════
# 2. SPEED VIEWS — aggregate streaming records
# ══════════════════════════════════════════════════════════════
speed_total = 0
try:
    df_speed = spark.read.parquet("hdfs://namenode:9000/realestate/streaming/listings")
    speed_total = df_speed.count()
    print(f"\n[SPEED] Streaming records: {speed_total:,}")

    # Speed view theo quận
    speed_district = df_speed.groupBy("district", "city").agg(
        F.count("*").alias("stream_listings"),
        F.round(F.avg("price_billion"), 3).alias("stream_avg_price"),
        F.round(F.avg("price_per_m2_million"), 1).alias("stream_avg_m2"),
    )
    speed_district.write.mode("overwrite").parquet("/data/speed_by_district.parquet")

    # Speed view theo loại BĐS
    speed_type = df_speed.groupBy("property_type").agg(
        F.count("*").alias("stream_listings"),
        F.round(F.avg("price_billion"), 3).alias("stream_avg_price"),
    )
    speed_type.write.mode("overwrite").parquet("/data/speed_by_type.parquet")

    # Speed summary (metadata)
    latest_ts = df_speed.agg(F.max("ingestion_time").alias("ts")).collect()[0]["ts"]
    spark.createDataFrame([{
        "stream_total":      int(speed_total),
        "batch_total":       int(batch_total),
        "latest_ingestion":  str(latest_ts),
    }]).write.mode("overwrite").parquet("/data/speed_summary.parquet")

    print(f"[SPEED] speed_by_district, speed_by_type, speed_summary OK")

    # ══════════════════════════════════════════════════════════
    # 3. SERVING LAYER — Merge batch + speed views
    # ══════════════════════════════════════════════════════════
    # Merge district
    df_batch_dist = spark.read.parquet("/data/by_district.parquet")
    merged_dist = df_batch_dist.join(speed_district, on=["district", "city"], how="full") \
        .fillna({"total_listings": 0, "stream_listings": 0,
                 "avg_price_billion": 0.0, "stream_avg_price": 0.0,
                 "avg_price_per_m2": 0.0})

    merged_dist = merged_dist \
        .withColumn("merged_listings",
            F.col("total_listings") + F.col("stream_listings")
        ) \
        .withColumn("merged_avg_price",
            F.round(
                (F.col("total_listings") * F.col("avg_price_billion") +
                 F.col("stream_listings") * F.col("stream_avg_price")) /
                F.greatest(
                    F.col("total_listings") + F.col("stream_listings"),
                    F.lit(1)
                ),
                3
            )
        )
    merged_dist.write.mode("overwrite").parquet("/data/merged_by_district.parquet")

    # Merge type
    df_batch_type = spark.read.parquet("/data/by_type.parquet")
    merged_type = df_batch_type.join(speed_type, on="property_type", how="full") \
        .fillna({"total_listings": 0, "stream_listings": 0,
                 "avg_price_billion": 0.0, "stream_avg_price": 0.0})

    merged_type = merged_type \
        .withColumn("merged_listings",
            F.col("total_listings") + F.col("stream_listings")
        ) \
        .withColumn("merged_avg_price",
            F.round(
                (F.col("total_listings") * F.col("avg_price_billion") +
                 F.col("stream_listings") * F.col("stream_avg_price")) /
                F.greatest(
                    F.col("total_listings") + F.col("stream_listings"),
                    F.lit(1)
                ),
                3
            )
        )
    merged_type.write.mode("overwrite").parquet("/data/merged_by_type.parquet")

    merged_total = batch_total + speed_total
    print(f"\n[SERVING] Merge hoàn tất:")
    print(f"  Batch   : {batch_total:,}")
    print(f"  Speed   : {speed_total:,}")
    print(f"  Merged  : {merged_total:,}")

except Exception as e:
    print(f"\n[WARN] Speed layer trống (chưa stream?): {e}")
    print("       Serving layer dùng batch-only.")

    # Ghi summary batch-only để Streamlit biết trạng thái
    spark.createDataFrame([{
        "stream_total":     0,
        "batch_total":      int(batch_total),
        "latest_ingestion": "N/A",
    }]).write.mode("overwrite").parquet("/data/speed_summary.parquet")

print("\nExport hoàn tất.")
spark.stop()
