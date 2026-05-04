from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "realestate_batch_pipeline",
    default_args=default_args,
    description="Lambda Architecture - Batch Layer - BDS Analytics",
    schedule_interval="@daily",     # Chạy hàng ngày để cập nhật tin đăng mới
    start_date=days_ago(1),
    tags=["realestate", "batch", "batdongsan"],
    catchup=False,
) as dag:

    SPARK        = "docker exec spark-master bash -c"
    SPARK_SUBMIT = "spark-submit --master spark://spark-master:7077"

    # Task 0: Chạy scraper lấy dữ liệu mới từ batdongsan.com
    scrape_data = BashOperator(
        task_id="scrape_batdongsan",
        bash_command=(
            "docker compose -f /path/to/Lambda-Architecture-RealEstate/docker-compose.yml "
            "run --rm scraper python scraper.py --pages 10 --delay 1.5"
        ),
    )

    # Task 1: Clean & chuẩn hóa CSV
    load_csv = BashOperator(
        task_id="load_csv",
        bash_command=f"docker exec spark-master python /spark-jobs/00_load_csv.py",
    )

    # Task 2: Upload lên HDFS
    upload_hdfs = BashOperator(
        task_id="upload_hdfs",
        bash_command=(
            'docker exec namenode bash -c "'
            "hdfs dfs -mkdir -p /realestate/raw && "
            "hdfs dfs -put -f /data/realestate_cleaned.csv /realestate/raw/realestate_cleaned.csv"
            '"'
        ),
    )

    # Task 3: Spark EDA
    spark_eda = BashOperator(
        task_id="spark_eda",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/01_eda.py"',
    )

    # Task 4: K-Means clustering
    spark_kmeans = BashOperator(
        task_id="spark_kmeans",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/02_kmeans.py"',
    )

    # Task 5: Hive queries
    hive_queries = BashOperator(
        task_id="hive_queries",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/04_hive_query.py"',
    )

    # Task 6: Export parquet về local cho Streamlit
    export_parquet = BashOperator(
        task_id="export_parquet",
        bash_command=f'{SPARK} "{SPARK_SUBMIT} /spark-jobs/05_export.py"',
    )

    scrape_data >> load_csv >> upload_hdfs >> spark_eda >> spark_kmeans >> hive_queries >> export_parquet
