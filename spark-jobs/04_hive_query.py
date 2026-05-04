"""
04_hive_query.py — Tạo Hive tables từ batch parquet, chạy aggregate queries.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("RealEstate Hive Query") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Tạo database ──────────────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS realestate")
spark.sql("USE realestate")

# ── 1. Bảng listings (toàn bộ dữ liệu cleaned) ───────────────
spark.sql("DROP TABLE IF EXISTS realestate.listings")
spark.sql("""
    CREATE EXTERNAL TABLE realestate.listings (
        listing_id           STRING,
        title                STRING,
        property_type        STRING,
        price_raw            STRING,
        price_billion        DOUBLE,
        area_m2              DOUBLE,
        price_per_m2_million DOUBLE,
        city                 STRING,
        district             STRING,
        ward                 STRING,
        address              STRING,
        bedrooms             INT,
        bathrooms            INT,
        floors               INT,
        direction            STRING,
        legal_status         STRING,
        posted_date          STRING,
        contact              STRING,
        url                  STRING,
        scraped_at           STRING,
        price_tier           STRING,
        area_tier            STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION 'hdfs://namenode:9000/realestate/raw_parquet'
""")

# Load data từ CSV → Hive
df_raw = spark.read.csv("hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
                        header=True, inferSchema=True)
df_raw.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/realestate/raw_parquet")

# ── 2. Thống kê theo quận ─────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.district_stats")
spark.sql("""
    CREATE TABLE realestate.district_stats AS
    SELECT
        district,
        city,
        COUNT(*) AS total_listings,
        ROUND(AVG(price_billion), 3) AS avg_price_billion,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2,
        ROUND(MIN(price_billion), 3) AS min_price,
        ROUND(MAX(price_billion), 3) AS max_price,
        ROUND(AVG(area_m2), 1) AS avg_area_m2
    FROM realestate.listings
    WHERE price_billion IS NOT NULL
    GROUP BY district, city
    ORDER BY total_listings DESC
""")

# ── 3. Thống kê theo loại BĐS ─────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.type_stats")
spark.sql("""
    CREATE TABLE realestate.type_stats AS
    SELECT
        property_type,
        COUNT(*) AS total_listings,
        ROUND(AVG(price_billion), 3) AS avg_price,
        ROUND(AVG(area_m2), 1) AS avg_area,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2,
        ROUND(AVG(bedrooms), 1) AS avg_bedrooms
    FROM realestate.listings
    WHERE price_billion IS NOT NULL
    GROUP BY property_type
    ORDER BY total_listings DESC
""")

# ── 4. Phân phối giá ─────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.price_distribution")
spark.sql("""
    CREATE TABLE realestate.price_distribution AS
    SELECT
        price_tier,
        COUNT(*) AS count,
        ROUND(AVG(area_m2), 1) AS avg_area,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2
    FROM realestate.listings
    WHERE price_tier IS NOT NULL
    GROUP BY price_tier
""")

# ── 5. Thống kê pháp lý ──────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.legal_stats")
spark.sql("""
    CREATE TABLE realestate.legal_stats AS
    SELECT
        legal_status,
        COUNT(*) AS count,
        ROUND(AVG(price_billion), 3) AS avg_price,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2
    FROM realestate.listings
    WHERE legal_status IS NOT NULL AND legal_status != 'None'
    GROUP BY legal_status
    ORDER BY count DESC
""")

# In kết quả sample
print("\n=== District Stats (Top 10) ===")
spark.sql("SELECT * FROM realestate.district_stats LIMIT 10").show(truncate=False)

print("\n=== Type Stats ===")
spark.sql("SELECT * FROM realestate.type_stats").show(truncate=False)

print("\n=== Price Distribution ===")
spark.sql("SELECT * FROM realestate.price_distribution").show(truncate=False)

print("Hive queries hoàn tất.")
spark.stop()
