"""
06_linear_regression.py — Dự đoán giá BĐS bằng Linear Regression (Spark MLlib).
Features: area_m2, bedrooms, bathrooms, floors, property_type, district
Label   : price_billion
Output  : lr_metrics, lr_coefficients, lr_predictions, lr_category_map, lr_model_params → HDFS
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import pandas as pd

spark = SparkSession.builder \
    .appName("RealEstate LinearRegression") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Load data ──────────────────────────────────────────────────
df = spark.read.csv(
    "hdfs://namenode:9000/realestate/raw/realestate_cleaned.csv",
    header=True, inferSchema=True
)

NUM_FEATURES = ["area_m2", "bedrooms", "bathrooms", "floors"]
CAT_FEATURES = ["property_type", "district"]
LABEL        = "price_billion"

df_model = df.select(NUM_FEATURES + CAT_FEATURES + [LABEL]).dropna()
total = df_model.count()
print(f"Tổng rows huấn luyện: {total:,}")

# ── Pipeline ───────────────────────────────────────────────────
type_indexer = StringIndexer(inputCol="property_type", outputCol="type_idx",
                             handleInvalid="skip")
dist_indexer = StringIndexer(inputCol="district",      outputCol="dist_idx",
                             handleInvalid="skip")

assembler = VectorAssembler(
    inputCols=NUM_FEATURES + ["type_idx", "dist_idx"],
    outputCol="features"
)

lr = LinearRegression(
    featuresCol="features", labelCol=LABEL,
    maxIter=100, regParam=0.1, elasticNetParam=0.0
)

pipeline = Pipeline(stages=[type_indexer, dist_indexer, assembler, lr])

# ── Train / Test split ─────────────────────────────────────────
train_df, test_df = df_model.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train_df)

print(f"Train: {train_df.count():,}  |  Test: {test_df.count():,}")

# ── Đánh giá trên tập test ────────────────────────────────────
preds = model.transform(test_df)

def eval_metric(metric):
    return RegressionEvaluator(
        labelCol=LABEL, predictionCol="prediction", metricName=metric
    ).evaluate(preds)

r2   = round(eval_metric("r2"),   4)
rmse = round(eval_metric("rmse"), 3)
mae  = round(eval_metric("mae"),  3)

print(f"\nR²   : {r2}")
print(f"RMSE : {rmse} tỷ")
print(f"MAE  : {mae} tỷ")

# ── 1. Metrics ─────────────────────────────────────────────────
metrics_pd = pd.DataFrame({
    "metric": ["R²", "RMSE (tỷ)", "MAE (tỷ)", "Train rows", "Test rows"],
    "value":  [r2,   rmse,        mae,         float(train_df.count()), float(test_df.count())]
})
spark.createDataFrame(metrics_pd) \
     .write.mode("overwrite") \
     .parquet("hdfs://namenode:9000/realestate/batch/lr_metrics")

# ── 2. Coefficients ────────────────────────────────────────────
lr_model = model.stages[-1]
feature_names = NUM_FEATURES + ["property_type_idx", "district_idx"]
coef_pd = pd.DataFrame({
    "feature":     feature_names,
    "coefficient": lr_model.coefficients.toArray().tolist(),
})
spark.createDataFrame(coef_pd) \
     .write.mode("overwrite") \
     .parquet("hdfs://namenode:9000/realestate/batch/lr_coefficients")

# ── 3. Predictions sample (actual vs predicted) ────────────────
preds_sample = preds.select(
    F.col(LABEL).alias("actual"),
    F.round("prediction", 3).alias("predicted"),
    "property_type", "district", "area_m2"
).limit(800)

preds_sample.write.mode("overwrite") \
    .parquet("hdfs://namenode:9000/realestate/batch/lr_predictions")

# ── 4. Category map (StringIndexer labels → dùng cho Streamlit predict) ───
type_labels = model.stages[0].labels
dist_labels = model.stages[1].labels

type_map = pd.DataFrame({
    "feature": "property_type",
    "label":   type_labels,
    "index":   [float(i) for i in range(len(type_labels))]
})
dist_map = pd.DataFrame({
    "feature": "district",
    "label":   dist_labels,
    "index":   [float(i) for i in range(len(dist_labels))]
})
cat_map_pd = pd.concat([type_map, dist_map], ignore_index=True)

spark.createDataFrame(cat_map_pd) \
     .write.mode("overwrite") \
     .parquet("hdfs://namenode:9000/realestate/batch/lr_category_map")

# ── 5. Model params (intercept + coefficients cho Streamlit predict) ───────
coefs = lr_model.coefficients.toArray()
params_pd = pd.DataFrame([{
    "intercept":          lr_model.intercept,
    "coef_area_m2":       coefs[0],
    "coef_bedrooms":      coefs[1],
    "coef_bathrooms":     coefs[2],
    "coef_floors":        coefs[3],
    "coef_property_type": coefs[4],
    "coef_district":      coefs[5],
}])

spark.createDataFrame(params_pd) \
     .write.mode("overwrite") \
     .parquet("hdfs://namenode:9000/realestate/batch/lr_model_params")

print("\nLinear Regression hoàn tất. Đã lưu HDFS:")
print("  lr_metrics, lr_coefficients, lr_predictions, lr_category_map, lr_model_params")
spark.stop()
