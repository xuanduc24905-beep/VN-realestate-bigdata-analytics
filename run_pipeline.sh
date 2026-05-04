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
echo "[0/8] Khởi động Docker services..."
docker compose up -d namenode datanode1 datanode2 resourcemanager nodemanager \
    spark-master spark-worker-1 spark-worker-2 \
    postgres hive-metastore hive-server \
    zookeeper kafka streamlit \
    airflow-postgres airflow-webserver airflow-scheduler

echo "      Đợi HDFS sẵn sàng (60s)..."
sleep 60

# ── Step 1: Scrape / sinh dữ liệu mẫu ───────────────────────
echo ""
echo "[1/8] Thu thập dữ liệu từ batdongsan.com (hoặc dữ liệu mẫu)..."
# Dùng --sample để sinh dữ liệu mẫu nhanh (không cần kết nối web)
# Bỏ --sample để scrape thật: docker compose run --rm scraper python scraper.py --pages 10
docker compose run --rm scraper python scraper.py --sample --sample-n 5000
echo "      [OK] realestate_raw.csv đã sẵn sàng"

# ── Step 2: Clean & chuẩn hóa ────────────────────────────────
echo ""
echo "[2/8] Load & clean dữ liệu (00_load_csv.py)..."
docker exec spark-master python /spark-jobs/00_load_csv.py
echo "      [OK] realestate_cleaned.csv"

# ── Step 3: Upload lên HDFS ───────────────────────────────────
echo ""
echo "[3/8] Upload CSV lên HDFS..."
docker exec namenode bash -c "
    hdfs dfs -mkdir -p /realestate/raw && \
    hdfs dfs -put -f /data/realestate_cleaned.csv /realestate/raw/realestate_cleaned.csv
"
echo "      [OK] hdfs:///realestate/raw/realestate_cleaned.csv"

# ── Step 4: Spark EDA ─────────────────────────────────────────
echo ""
echo "[4/8] Spark EDA (01_eda.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/01_eda.py"
echo "      [OK] batch stats → HDFS /realestate/batch/"

# ── Step 5: K-Means ───────────────────────────────────────────
echo ""
echo "[5/8] K-Means Clustering (02_kmeans.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/02_kmeans.py"
echo "      [OK] 5 phân khúc đã xác định"

# ── Step 6: Linear Regression ────────────────────────────────
echo ""
echo "[6/8] Linear Regression (06_linear_regression.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/06_linear_regression.py"
echo "      [OK] lr_metrics, lr_coefficients, lr_predictions → HDFS"

# ── Step 7: Hive ─────────────────────────────────────────────
echo ""
echo "[7/8] Hive Queries + Serving Layer (04_hive_query.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/04_hive_query.py"
echo "      [OK] Hive tables + merged_listings view đã tạo"

# ── Step 8: Export ────────────────────────────────────────────
echo ""
echo "[8/8] Export + Merge Serving Layer (05_export.py)..."
$SPARK "$SPARK_SUBMIT /spark-jobs/05_export.py"
echo "      [OK] /data/*.parquet (batch + speed merged)"

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
