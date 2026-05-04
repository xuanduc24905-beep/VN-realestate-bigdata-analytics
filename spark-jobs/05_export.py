"""
05_export.py — Export kết quả batch từ HDFS về /data/*.parquet cho Streamlit đọc.
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RealEstate Export") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

EXPORTS = {
    "hdfs://namenode:9000/realestate/batch/by_district":   "/data/by_district.parquet",
    "hdfs://namenode:9000/realestate/batch/by_type":       "/data/by_type.parquet",
    "hdfs://namenode:9000/realestate/batch/price_dist":    "/data/price_dist.parquet",
    "hdfs://namenode:9000/realestate/batch/cluster_stats": "/data/cluster_stats.parquet",
    "hdfs://namenode:9000/realestate/batch/clustered":     "/data/clustered.parquet",
    "hdfs://namenode:9000/realestate/batch/by_legal":      "/data/by_legal.parquet",
    "hdfs://namenode:9000/realestate/batch/by_direction":  "/data/by_direction.parquet",
}

# Cũng export cleaned CSV → parquet cho dashboard batch
df_cleaned = spark.read.csv(
    "hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
    header=True, inferSchema=True
)
df_cleaned.write.mode("overwrite").parquet("/data/cleaned.parquet")
print(f"Exported cleaned: {df_cleaned.count()} rows → /data/cleaned.parquet")

for hdfs_path, local_path in EXPORTS.items():
    try:
        df = spark.read.parquet(hdfs_path)
        df.write.mode("overwrite").parquet(local_path)
        print(f"Exported {hdfs_path.split('/')[-1]}: {df.count()} rows → {local_path}")
    except Exception as e:
        print(f"[WARN] Bỏ qua {hdfs_path}: {e}")

print("\nExport hoàn tất.")
spark.stop()
