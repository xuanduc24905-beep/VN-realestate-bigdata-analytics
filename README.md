# Lambda Architecture — Real Estate Analytics

Hệ thống phân tích dữ liệu bất động sản Việt Nam theo kiến trúc Lambda.
Dữ liệu thu thập từ **batdongsan.com**.

## Stack

| Layer | Công nghệ |
|-------|-----------|
| Ingest | Python Scraper (BeautifulSoup) |
| Storage | HDFS (Hadoop 3.2) |
| Batch | Apache Spark 3.5 + Hive 4.0 |
| Speed | Apache Kafka + Spark Structured Streaming |
| Serving | Hive Metastore (PostgreSQL) |
| Orchestration | Apache Airflow 2.8 |
| Dashboard | Streamlit |

## Cấu trúc thư mục

```
Lambda-Architecture-RealEstate/
├── scraper/                    # Thu thập dữ liệu batdongsan.com
│   ├── scraper.py
│   ├── requirements.txt
│   └── Dockerfile
├── spark-jobs/
│   ├── 00_load_csv.py          # Clean & chuẩn hóa dữ liệu
│   ├── 01_eda.py               # EDA: thống kê giá, diện tích, vị trí
│   ├── 02_kmeans.py            # K-Means phân khúc thị trường
│   ├── 03_kafka_producer.py    # Streaming producer
│   ├── 03_spark_streaming.py   # Spark Structured Streaming consumer
│   ├── 04_hive_query.py        # Tạo Hive tables & aggregate
│   └── 05_export.py            # Export parquet về local
├── dags/
│   └── realestate_batch_dag.py # Airflow DAG
├── streamlit-app/
│   ├── app.py                  # Dashboard Streamlit
│   ├── requirements.txt
│   └── Dockerfile
├── conf/
│   ├── hive-site.xml
│   └── postgresql-42.7.3.jar
├── data/                       # CSV và parquet output (gitignored)
├── docker-compose.yml
└── run_pipeline.sh             # One-click runner
```

## Schema dữ liệu

| Cột | Mô tả |
|-----|-------|
| listing_id | ID tin đăng |
| title | Tiêu đề |
| property_type | Loại BĐS (nhà phố, căn hộ, đất nền...) |
| price_billion | Giá (tỷ VND) |
| area_m2 | Diện tích (m²) |
| price_per_m2_million | Giá/m² (triệu VND) |
| district / city | Quận/huyện và thành phố |
| bedrooms / bathrooms / floors | Số phòng |
| direction | Hướng nhà |
| legal_status | Tình trạng pháp lý |
| price_tier / area_tier | Phân khúc giá/diện tích |

## Chạy nhanh

```bash
# 1. Khởi động toàn bộ pipeline (batch + services)
./run_pipeline.sh

# 2. Streaming (2 terminal riêng)
docker exec spark-master python /spark-jobs/03_kafka_producer.py
docker exec spark-master spark-submit --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /spark-jobs/03_spark_streaming.py

# 3. Xem dashboard
open http://localhost:8501
```

## Scraper thật vs dữ liệu mẫu

```bash
# Dùng dữ liệu mẫu (nhanh, không cần internet)
docker compose run --rm scraper python scraper.py --sample --sample-n 5000

# Scrape thật từ batdongsan.com
docker compose run --rm scraper python scraper.py --pages 10 --delay 2.0
```

## Web UIs

| Service | URL |
|---------|-----|
| Dashboard | http://localhost:8501 |
| Spark Master | http://localhost:8080 |
| HDFS NameNode | http://localhost:9870 |
| YARN | http://localhost:8088 |
| Airflow | http://localhost:8083 |
| HiveServer2 | http://localhost:10002 |
