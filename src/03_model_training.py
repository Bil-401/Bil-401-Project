import json

from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

from project_config import TEST_END, TEST_START, create_spark_session, data_path, reports_path

spark = create_spark_session("NYC_Taxi_GBT")

df = spark.read.parquet(data_path("feature_store"))

# Feature vektörü oluştur
feature_cols = ["hour_of_day", "day_of_week", "lag_1h", "lag_24h", "rolling_avg_3h"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

# Tam yıl için kronolojik ayrım (shuffle yok):
# Ocak-Kasım eğitim, Aralık test.
train = df.filter(col("hour_start") < TEST_START)
test = df.filter(
    (col("hour_start") >= TEST_START) & (col("hour_start") < TEST_END)
)

# Model
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="demand",
    maxIter=50,
    maxDepth=5
)

# Pipeline
pipeline = Pipeline(stages=[assembler, gbt])
model = pipeline.fit(train)

# Değerlendirme
predictions = model.transform(test)

evaluator_rmse = RegressionEvaluator(
    labelCol="demand", predictionCol="prediction", metricName="rmse"
)
evaluator_r2 = RegressionEvaluator(
    labelCol="demand", predictionCol="prediction", metricName="r2"
)

gbt_rmse = evaluator_rmse.evaluate(predictions)
gbt_r2 = evaluator_r2.evaluate(predictions)

# A persistence baseline puts the GBT result in context: predict the demand at
# the same zone and hour one day earlier.
baseline_predictions = test.withColumn("prediction", col("lag_24h").cast("double"))
baseline_rmse = evaluator_rmse.evaluate(baseline_predictions)
baseline_r2 = evaluator_r2.evaluate(baseline_predictions)

metrics = {
    "split": {
        "train_before": TEST_START,
        "test_from": TEST_START,
        "test_until": TEST_END,
    },
    "gbt": {"rmse": gbt_rmse, "r2": gbt_r2},
    "lag_24h_baseline": {"rmse": baseline_rmse, "r2": baseline_r2},
}

print(f"GBT RMSE: {gbt_rmse:.2f}")
print(f"GBT R²  : {gbt_r2:.4f}")
print(f"Lag-24h baseline RMSE: {baseline_rmse:.2f}")
print(f"Lag-24h baseline R²  : {baseline_r2:.4f}")

with open(reports_path("training_metrics.json"), "w", encoding="utf-8") as metrics_file:
    json.dump(metrics, metrics_file, indent=2)

model.write().overwrite().save(data_path("model", "gbt_taxi_demand"))
spark.stop()
