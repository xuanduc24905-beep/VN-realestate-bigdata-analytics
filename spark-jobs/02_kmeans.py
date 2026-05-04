"""
02_kmeans.py — Phân cụm bất động sản bằng K-Means (Spark MLlib).
Features: price_billion, area_m2, price_per_m2_million, bedrooms
Clusters: 5 phân khúc thị trường (bình dân → cao cấp)
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

spark = SparkSession.builder \
    .appName("RealEstate KMeans") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("hdfs://namenode:9000/realestate/batch/by_type").count()
# Đọc data cleaned cho KMeans
df = spark.read.csv("hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
                    header=True, inferSchema=True)

FEATURES = ["price_billion", "area_m2", "price_per_m2_million", "bedrooms"]

df_ml = df.select(["listing_id", "property_type", "district", "city"] + FEATURES) \
          .dropna(subset=FEATURES)

assembler = VectorAssembler(inputCols=FEATURES, outputCol="raw_features")
scaler    = StandardScaler(inputCol="raw_features", outputCol="features",
                           withMean=True, withStd=True)
kmeans    = KMeans(featuresCol="features", predictionCol="cluster",
                   k=5, seed=42, maxIter=30)

pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model    = pipeline.fit(df_ml)
df_clustered = model.transform(df_ml)

# Đặt tên phân khúc dựa trên giá trung bình của từng cluster
cluster_stats = df_clustered.groupBy("cluster").agg(
    F.count("*").alias("count"),
    F.round(F.avg("price_billion"), 3).alias("avg_price"),
    F.round(F.avg("area_m2"), 1).alias("avg_area"),
    F.round(F.avg("price_per_m2_million"), 1).alias("avg_price_per_m2"),
).orderBy("avg_price")

cluster_stats.show()

# Map cluster id → tên phân khúc theo thứ tự giá tăng dần
SEGMENT_NAMES = ["Bình dân", "Tầm trung", "Khá", "Cao cấp", "Siêu cao cấp"]
rows = cluster_stats.collect()
cluster_map = {row["cluster"]: SEGMENT_NAMES[i] for i, row in enumerate(rows)}

mapping_expr = F.create_map([F.lit(x) for pair in cluster_map.items() for x in pair])
df_clustered = df_clustered.withColumn("segment", mapping_expr[F.col("cluster")])

df_clustered.drop("raw_features", "features") \
    .write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/realestate/batch/clustered")

cluster_stats_named = cluster_stats.join(
    spark.createDataFrame(list(cluster_map.items()), ["cluster", "segment"]),
    on="cluster"
)
cluster_stats_named.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/realestate/batch/cluster_stats")

print("KMeans hoàn tất. 5 phân khúc:")
cluster_stats_named.show(truncate=False)
spark.stop()
