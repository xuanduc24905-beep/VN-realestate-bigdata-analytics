"""
04_hive_query.py — Serving Layer (Hive):
  - Batch views: listings, district_stats, type_stats, price_distribution, legal_stats
  - Speed view : streaming_listings (EXTERNAL table → HDFS streaming path)
  - Merged view: merged_listings (UNION batch + stream) → merged_district_stats
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

# ── Database ───────────────────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS realestate")
spark.sql("USE realestate")

# ══════════════════════════════════════════════════════════════
# BATCH LAYER — Batch Views
# ══════════════════════════════════════════════════════════════

# ── 1. listings (toàn bộ batch) ───────────────────────────────
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

df_raw = spark.read.csv(
    "hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
    header=True, inferSchema=True
)
df_raw.write.mode("overwrite").parquet("hdfs://namenode:9000/realestate/raw_parquet")
batch_total = df_raw.count()
print(f"Batch listings: {batch_total:,} rows")

# ── 2. district_stats ─────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.district_stats")
spark.sql("""
    CREATE TABLE realestate.district_stats AS
    SELECT
        district, city,
        COUNT(*) AS total_listings,
        ROUND(AVG(price_billion), 3)        AS avg_price_billion,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2,
        ROUND(MIN(price_billion), 3)        AS min_price,
        ROUND(MAX(price_billion), 3)        AS max_price,
        ROUND(AVG(area_m2), 1)             AS avg_area_m2
    FROM realestate.listings
    WHERE price_billion IS NOT NULL
    GROUP BY district, city
    ORDER BY total_listings DESC
""")

# ── 3. type_stats ─────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.type_stats")
spark.sql("""
    CREATE TABLE realestate.type_stats AS
    SELECT
        property_type,
        COUNT(*) AS total_listings,
        ROUND(AVG(price_billion), 3)        AS avg_price,
        ROUND(AVG(area_m2), 1)             AS avg_area,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2,
        ROUND(AVG(bedrooms), 1)            AS avg_bedrooms
    FROM realestate.listings
    WHERE price_billion IS NOT NULL
    GROUP BY property_type
    ORDER BY total_listings DESC
""")

# ── 4. price_distribution ────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.price_distribution")
spark.sql("""
    CREATE TABLE realestate.price_distribution AS
    SELECT
        price_tier,
        COUNT(*) AS count,
        ROUND(AVG(area_m2), 1)             AS avg_area,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2
    FROM realestate.listings
    WHERE price_tier IS NOT NULL
    GROUP BY price_tier
""")

# ── 5. legal_stats ────────────────────────────────────────────
spark.sql("DROP TABLE IF EXISTS realestate.legal_stats")
spark.sql("""
    CREATE TABLE realestate.legal_stats AS
    SELECT
        legal_status,
        COUNT(*) AS count,
        ROUND(AVG(price_billion), 3)        AS avg_price,
        ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2
    FROM realestate.listings
    WHERE legal_status IS NOT NULL AND legal_status != 'None'
    GROUP BY legal_status
    ORDER BY count DESC
""")

print("Batch views OK: district_stats, type_stats, price_distribution, legal_stats")

# ══════════════════════════════════════════════════════════════
# SPEED LAYER — Streaming table (External → HDFS speed path)
# ══════════════════════════════════════════════════════════════
try:
    spark.sql("DROP TABLE IF EXISTS realestate.streaming_listings")
    spark.sql("""
        CREATE EXTERNAL TABLE realestate.streaming_listings (
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
            area_tier            STRING,
            kafka_timestamp      TIMESTAMP,
            ingestion_time       TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION 'hdfs://namenode:9000/realestate/streaming/listings'
    """)
    stream_total = spark.sql("SELECT COUNT(*) AS cnt FROM realestate.streaming_listings").collect()[0]["cnt"]
    print(f"Speed layer — streaming_listings: {stream_total:,} records")

    # ══════════════════════════════════════════════════════════
    # SERVING LAYER — Merged view (Batch UNION Speed)
    # ══════════════════════════════════════════════════════════
    spark.sql("DROP VIEW IF EXISTS realestate.merged_listings")
    spark.sql("""
        CREATE VIEW realestate.merged_listings AS
        SELECT
            listing_id, title, property_type, price_raw, price_billion,
            area_m2, price_per_m2_million, city, district, ward, address,
            bedrooms, bathrooms, floors, direction, legal_status,
            posted_date, price_tier, area_tier,
            'batch' AS data_source
        FROM realestate.listings
        UNION ALL
        SELECT
            listing_id, title, property_type, price_raw, price_billion,
            area_m2, price_per_m2_million, city, district, ward, address,
            bedrooms, bathrooms, floors, direction, legal_status,
            posted_date, price_tier, area_tier,
            'stream' AS data_source
        FROM realestate.streaming_listings
    """)

    # Merged district stats (dùng trong Serving Layer)
    spark.sql("DROP TABLE IF EXISTS realestate.merged_district_stats")
    spark.sql("""
        CREATE TABLE realestate.merged_district_stats AS
        SELECT
            district, city, data_source,
            COUNT(*) AS total_listings,
            ROUND(AVG(price_billion), 3)        AS avg_price_billion,
            ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2,
            ROUND(AVG(area_m2), 1)             AS avg_area_m2
        FROM realestate.merged_listings
        WHERE price_billion IS NOT NULL
        GROUP BY district, city, data_source
        ORDER BY total_listings DESC
    """)

    # Merged type stats
    spark.sql("DROP TABLE IF EXISTS realestate.merged_type_stats")
    spark.sql("""
        CREATE TABLE realestate.merged_type_stats AS
        SELECT
            property_type, data_source,
            COUNT(*) AS total_listings,
            ROUND(AVG(price_billion), 3)        AS avg_price_billion,
            ROUND(AVG(price_per_m2_million), 1) AS avg_price_per_m2
        FROM realestate.merged_listings
        WHERE price_billion IS NOT NULL
        GROUP BY property_type, data_source
        ORDER BY total_listings DESC
    """)

    print("Serving layer — merged_listings view, merged_district_stats, merged_type_stats OK")
    spark.sql("""
        SELECT data_source, COUNT(*) as records
        FROM realestate.merged_listings
        GROUP BY data_source
    """).show()

except Exception as e:
    print(f"[WARN] Speed/Serving layer bị bỏ qua (chưa có streaming data): {e}")

# ── Summary ───────────────────────────────────────────────────
print("\n=== District Stats Top 10 ===")
spark.sql("SELECT * FROM realestate.district_stats LIMIT 10").show(truncate=False)

print("\n=== Type Stats ===")
spark.sql("SELECT * FROM realestate.type_stats").show(truncate=False)

print("\nHive Serving Layer hoàn tất.")
spark.stop()
