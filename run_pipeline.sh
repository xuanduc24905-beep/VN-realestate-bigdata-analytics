#!/usr/bin/env bash
# ============================================================
#  run_pipeline.sh — Chạy toàn bộ Lambda Architecture pipeline
#  BĐS Analytics (batdongsan.com)
# ============================================================
set -e

SPARK="docker exec spark-master bash -c"
SPARK_SUBMIT="spark-submit --master spark://spark-master:7077"

echo "======================================================"
echo " Lambda Architecture — BDS Analytics"
echo "======================================================"

# ── Step 0: Khởi động tất cả services ───────────────────────
echo ""
echo "[0/7] Khởi động Docker services..."
docker compose up -d namenode datanode1 datanode2 resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    postgres hive-metastore hive-server \
    zookeeper kafka streamlit \
    airflow-postgres airflow-webserver airflow-scheduler

echo "      Đợi HDFS sẵn sàng (60s)..."
sleep 60

# ── Step 1: Scrape / sinh dữ liệu mẫu ───────────────────────
echo ""
echo "[1/7] Thu thập dữ liệu từ batdongsan.com (hoặc dữ liệu mẫu)..."
# Dùng --sample để sinh dữ liệu mẫu nhanh (không cần kết nối web)
# Bỏ --sample để scrape thật: docker compose run --rm scraper python scraper.py --pages 10
docker compose run --rm scraper python scraper.py --sample --sample-n 5000
echo "      [OK] realestate_raw.csv đã sẵn sàng"

# ── Step 2: Clean & chuẩn hóa ────────────────────────────────
echo ""
echo "[2/7] Load & clean dữ liệu (00_load_csv.py)..."
docker exec spark-master python /spark-jobs/00_load_csv.py
echo "      [OK] realestate_cleaned.csv"

# ── Step 3: Upload lên HDFS ───────────────────────────────────
echo ""
echo "[3/7] Upload CSV lên HDFS..."
docker exec namenode bash -c "
    hdfs dfs -mkdir -p /realestate/raw && \
    hdfs dfs -put -f /data/realestate_cleaned.csv /realestate/raw/realestate_cleaned.csv
"
echo "      [OK] hdfs:///realestate/raw/realestate_cleaned.csv"

# ── Step 4: Spark EDA ─────────────────────────────────────────
echo ""
echo "[4/7] Spark EDA (01_eda.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"
echo "      [OK] batch stats → HDFS /realestate/batch/"

# ── Step 5: K-Means ───────────────────────────────────────────
echo ""
echo "[5/7] K-Means Clustering (02_kmeans.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_kmeans.py"
echo "      [OK] 5 phân khúc đã xác định"

# ── Step 6: Hive ─────────────────────────────────────────────
echo ""
echo "[6/7] Hive Queries (04_hive_query.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_query.py"
echo "      [OK] Hive tables đã tạo"

# ── Step 7: Export ────────────────────────────────────────────
echo ""
echo "[7/7] Export parquet cho Streamlit (05_export.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"
echo "      [OK] /data/*.parquet"

echo ""
echo "======================================================"
echo " BATCH PIPELINE HOÀN TẤT!"
echo "======================================================"
echo ""
echo " Streamlit Dashboard : http://localhost:8501"
echo " Spark Master UI     : http://localhost:8080"
echo " HDFS NameNode UI    : http://localhost:9870"
echo " YARN ResourceMgr    : http://localhost:8088"
echo " Airflow             : http://localhost:8083  (admin/admin)"
echo " HiveServer2 UI      : http://localhost:10002"
echo ""
echo " STREAMING (chạy 2 terminal riêng):"
echo "   Producer : docker exec spark-master python /spark-jobs/03_kafka_producer.py"
echo "   Consumer : docker exec spark-master $SPARK_SUBMIT /spark-jobs/03_spark_streaming.py"
echo ""
