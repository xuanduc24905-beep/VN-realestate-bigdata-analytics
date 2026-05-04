# VN Real Estate BigData Analytics

Hệ thống phân tích dữ liệu **bất động sản Việt Nam** theo kiến trúc **Lambda Architecture**.  
Dữ liệu thu thập tự động từ **batdongsan.com**, xử lý theo 2 luồng song song: **Batch** (lịch sử) và **Speed** (real-time), hiển thị trên dashboard Streamlit.

---

## Lambda Architecture

```
                    ┌──────────────────────────────────────┐
                    │      batdongsan.com  (~N listings)    │
                    │   scraper.py  (BeautifulSoup)         │
                    └──────────────┬───────────────────────┘
                                   │ realestate_raw.csv
                    ┌──────────────▼───────────────────────┐
                    │         00_load_csv.py                │
                    │   Schema mapping + decade column      │
                    └──────┬───────────────────┬───────────┘
                           │                   │
              ┌────────────▼──────┐   ┌────────▼─────────────┐
              │   BATCH LAYER     │   │     SPEED LAYER       │
              │                   │   │                       │
              │  HDFS /raw/       │   │  03_kafka_producer.py │
              │       │           │   │  (CSV → Kafka topic)  │
              │  01_eda.py        │   │         │             │
              │  (EDA + stats)    │   │  03_spark_streaming.py│
              │       │           │   │  (Kafka → HDFS append)│
              │  02_kmeans.py     │   │         │             │
              │  (K-Means k=3~8)  │   │  HDFS /streaming/     │
              │       │           │   └────────┬──────────────┘
              │  04_hive_query.py │            │
              │  (Hive tables)    │            │
              │       │           │            │
              │  05_export.py     │            │
              │  (HDFS → parquet) │            │
              └────────┬──────────┘            │
                       │                       │
              ┌────────▼───────────────────────▼──────────┐
              │              SERVING LAYER                  │
              │   Hive Metastore (PostgreSQL backend)       │
              │   HDFS Parquet files → /data/*.parquet      │
              └────────────────────┬────────────────────────┘
                                   │
                      ┌────────────▼────────────┐
                      │    Streamlit Dashboard   │
                      │  http://localhost:8501   │
                      │                         │
                      │ • Giá theo quận/huyện   │
                      │ • Phân khúc K-Means     │
                      │ • Phân phối giá/DT      │
                      │ • Pháp lý & hướng nhà  │
                      │ • Real-time feed        │
                      └─────────────────────────┘
```

---

## Tech Stack

| Layer | Công nghệ | Phiên bản |
|-------|-----------|-----------|
| **Ingest** | Python Scraper (BeautifulSoup + requests) | Python 3.10 |
| **Storage** | Apache Hadoop HDFS | 3.2.1 |
| **Batch Processing** | Apache Spark | 3.5.0 |
| **Data Warehouse** | Apache Hive | 4.0.0 |
| **Message Queue** | Apache Kafka + Zookeeper | 7.4.0 |
| **Stream Processing** | Spark Structured Streaming | 3.5.0 |
| **Metastore Backend** | PostgreSQL | 15 |
| **Orchestration** | Apache Airflow | 2.8.1 |
| **Dashboard** | Streamlit + Plotly | 1.32.0 |
| **Container** | Docker + Docker Compose | - |

---

## Cấu trúc thư mục

```
VN-realestate-bigdata-analytics/
│
├── scraper/                        # Thu thập dữ liệu batdongsan.com
│   ├── scraper.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark-jobs/                     # Toàn bộ Spark jobs (theo thứ tự chạy)
│   ├── 00_load_csv.py              # Clean & chuẩn hóa schema
│   ├── 01_eda.py                   # EDA: giá, diện tích, quận, pháp lý
│   ├── 02_kmeans.py                # K-Means 5 phân khúc thị trường
│   ├── 03_kafka_producer.py        # [Speed] Phát tin đăng vào Kafka
│   ├── 03_spark_streaming.py       # [Speed] Spark Structured Streaming consumer
│   ├── 04_hive_query.py            # Tạo Hive tables & aggregate queries
│   └── 05_export.py                # Export parquet về /data/ cho Streamlit
│
├── dags/
│   └── realestate_batch_dag.py     # Airflow DAG (@daily)
│
├── streamlit-app/
│   ├── app.py                      # Dashboard chính (5 trang)
│   ├── requirements.txt
│   └── Dockerfile
│
├── conf/
│   ├── hive-site.xml               # Hive metastore config
│   └── postgresql-42.7.3.jar       # JDBC driver
│
├── data/                           # Output CSV & parquet (gitignored)
├── docker-compose.yml              # 15 services
└── run_pipeline.sh                 # One-click batch runner
```

---

## Yêu cầu hệ thống

- **Docker Desktop** ≥ 4.x + Docker Compose v2
- **RAM**: 16 GB tối thiểu, khuyến nghị **32 GB+**
- **Disk**: 20 GB+ trống
- **OS**: Linux / macOS / Windows (WSL2)

---

## Cài đặt & Chạy Docker

### 1. Clone repo

```bash
git clone https://github.com/xuanduc24905-beep/VN-realestate-bigdata-analytics.git
cd VN-realestate-bigdata-analytics
```

### 2. Khởi động toàn bộ services

```bash
docker compose up -d
```

> **Lần đầu** sẽ mất 5–15 phút để pull images (~8 GB). Các lần sau chỉ mất ~1–2 phút.

Kiểm tra trạng thái:

```bash
docker compose ps
```

Tất cả services cần đạt trạng thái `running` hoặc `healthy` trước khi chạy pipeline.

### 3. Xử lý lỗi "container name already in use"

Nếu gặp lỗi:
```
Error: Conflict. The container name "/zookeeper" is already in use...
```

Nguyên nhân: containers từ lần chạy trước chưa được dọn dẹp. Chạy lệnh sau để reset sạch:

```bash
# Dừng và xóa toàn bộ containers cũ
docker compose down

# Khởi động lại
docker compose up -d
```

Nếu vẫn lỗi (containers từ project khác dùng cùng tên):

```bash
# Xóa container cụ thể đang xung đột
docker rm -f zookeeper kafka namenode

# Rồi chạy lại
docker compose up -d
```

### 4. Xem logs theo dõi quá trình khởi động

Hai service khởi động chậm nhất là `namenode` và `hive-metastore` (~2 phút). Theo dõi:

```bash
# Xem log tất cả services
docker compose logs -f

# Xem log service cụ thể
docker compose logs -f namenode
docker compose logs -f hive-metastore
docker compose logs -f airflow-webserver
```

Dấu hiệu sẵn sàng:
- `namenode` → thấy `"IPC Server Listener on 9000"`
- `hive-metastore` → thấy `"Starting Hive Metastore Server"`
- `airflow-webserver` → thấy `"Booting worker"`

### 5. Dừng và khởi động lại

```bash
# Dừng toàn bộ (giữ dữ liệu volumes)
docker compose stop

# Khởi động lại (không mất dữ liệu)
docker compose start

# Dừng + xóa containers (giữ volumes)
docker compose down

# Dừng + xóa cả volumes (RESET HOÀN TOÀN)
docker compose down -v
```

---

## Chạy Pipeline

### Batch Pipeline (one-click)

```bash
./run_pipeline.sh
```

Script tự động:
1. Khởi động Docker services
2. Thu thập dữ liệu (mặc định 5000 dòng mẫu)
3. Clean & upload lên HDFS
4. Spark EDA + K-Means clustering
5. Tạo Hive tables
6. Export parquet → `/data/` cho Streamlit

### Chạy từng bước thủ công

```bash
# Bước 1 — Thu thập dữ liệu mẫu (nhanh, không cần internet)
docker compose run --rm scraper python scraper.py --sample --sample-n 5000

# Bước 2 — Load & clean CSV → HDFS
docker exec spark-master spark-submit /spark-jobs/00_load_csv.py

# Bước 3 — EDA
docker exec spark-master spark-submit /spark-jobs/01_eda.py

# Bước 4 — K-Means clustering
docker exec spark-master spark-submit /spark-jobs/02_kmeans.py

# Bước 5 — Hive tables
docker exec spark-master spark-submit /spark-jobs/04_hive_query.py

# Bước 6 — Export parquet
docker exec spark-master spark-submit /spark-jobs/05_export.py
```

### Real-time Streaming (2 terminal)

```bash
# Terminal 1 — Kafka Producer (phát tin đăng liên tục)
docker exec spark-master python /spark-jobs/03_kafka_producer.py

# Terminal 2 — Spark Streaming Consumer
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /spark-jobs/03_spark_streaming.py
```

### Scraper: Real vs Sample mode

```bash
# Dữ liệu mẫu — không cần internet, dùng để test
docker compose run --rm scraper python scraper.py --sample --sample-n 5000

# Scrape thật từ batdongsan.com
docker compose run --rm scraper python scraper.py --pages 10 --delay 2.0

# Scrape + lấy chi tiết từng tin (bedrooms, direction, legal...)
docker compose run --rm scraper python scraper.py --pages 5 --detail
```

---

## Web UIs

| Service | URL | Tài khoản |
|---------|-----|-----------|
| **Streamlit Dashboard** | http://localhost:8501 | — |
| **Spark Master** | http://localhost:8080 | — |
| **HDFS NameNode** | http://localhost:9870 | — |
| **YARN ResourceManager** | http://localhost:8088 | — |
| **Airflow** | http://localhost:8083 | admin / admin |
| **HiveServer2** | http://localhost:10002 | — |

---

## Schema dữ liệu

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `listing_id` | STRING | ID duy nhất của tin đăng |
| `title` | STRING | Tiêu đề tin đăng |
| `property_type` | STRING | Nhà phố / Căn hộ / Đất nền / Biệt thự / Nhà riêng |
| `price_billion` | DOUBLE | Giá bán (tỷ VND) |
| `area_m2` | DOUBLE | Diện tích (m²) |
| `price_per_m2_million` | DOUBLE | Giá/m² (triệu VND/m²) |
| `city` | STRING | Thành phố |
| `district` | STRING | Quận/Huyện |
| `ward` | STRING | Phường/Xã |
| `address` | STRING | Địa chỉ đầy đủ |
| `bedrooms` | INT | Số phòng ngủ |
| `bathrooms` | INT | Số phòng tắm |
| `floors` | INT | Số tầng |
| `direction` | STRING | Hướng nhà (Đông / Tây / Nam / Bắc...) |
| `legal_status` | STRING | Sổ đỏ / Sổ hồng / Đang chờ sổ... |
| `price_tier` | STRING | Dưới 2 tỷ / 2–5 tỷ / 5–10 tỷ / 10–20 tỷ / Trên 20 tỷ |
| `area_tier` | STRING | Dưới 40m² / 40–80m² / 80–150m² / 150–300m² / Trên 300m² |
| `posted_date` | STRING | Ngày đăng tin |
| `scraped_at` | STRING | Thời điểm scrape |

---

## Dashboard — Các biểu đồ

| Trang | Biểu đồ |
|-------|---------|
| **Tổng quan** | Giá TB theo quận, phân bổ loại BĐS, phân phối khoảng giá |
| **Phân tích giá** | Giá/m² theo loại BĐS, heatmap giá theo quận |
| **K-Means** | Scatter plot 5 phân khúc thị trường |
| **Pháp lý & Hướng** | Pie chart pháp lý, polar bar hướng nhà |
| **Real-time** | Feed tin đăng mới nhất từ Kafka stream |

---

## Troubleshooting

| Lỗi | Nguyên nhân | Giải pháp |
|-----|-------------|-----------|
| `container name already in use` | Container cũ chưa xóa | `docker compose down && docker compose up -d` |
| `namenode` không healthy | HDFS chưa khởi tạo xong | Đợi thêm 1–2 phút, kiểm tra log |
| `hive-metastore` exit | Postgres chưa sẵn sàng | `docker compose restart hive-metastore` |
| Streamlit trắng trang | Chưa chạy `05_export.py` | Chạy batch pipeline trước |
| Port bị chiếm | Ứng dụng khác dùng port | `lsof -i :8080` rồi kill process |
| RAM không đủ | Spark worker cần 24GB × 2 | Giảm `SPARK_WORKER_MEMORY` trong `docker-compose.yml` |
