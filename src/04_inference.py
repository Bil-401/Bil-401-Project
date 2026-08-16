import json

from pyspark.sql.functions import col
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator

from project_config import TEST_END, TEST_START, create_spark_session, data_path, reports_path

spark = create_spark_session("NYC_Taxi_Inference")

# Eğitilmiş modeli yükle
model = PipelineModel.load(data_path("model", "gbt_taxi_demand"))

# Feature store'dan özellikleri oku (yeniden hesaplamaya gerek yok)
features = spark.read.parquet(data_path("feature_store"))

# Aralık 2019 verisi üzerinde batch inference
inference_data = features.filter(
    (col("hour_start") >= TEST_START) & (col("hour_start") < TEST_END)
)

# Tahmin yap
predictions = model.transform(inference_data)

# Sonuçları kaydet
predictions.select(
    "hour_start",
    "PULocationID",
    "Borough",
    "Zone",
    "demand",
    "prediction"
).write.mode("overwrite").parquet(data_path("predictions"))

# Özet metrikler yazdır
evaluator_rmse = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator_r2 = RegressionEvaluator(
    labelCol="demand",
    predictionCol="prediction",
    metricName="r2"
)

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

record_count = predictions.count()
print("Inference tamamlandı.")
print(f"RMSE : {rmse:.2f}")
print(f"R²   : {r2:.4f}")
print(f"Tahmin edilen kayıt sayısı: {record_count:,}")

with open(reports_path("inference_metrics.json"), "w", encoding="utf-8") as metrics_file:
    json.dump(
        {
            "period_start": TEST_START,
            "period_end": TEST_END,
            "records": record_count,
            "rmse": rmse,
            "r2": r2,
        },
        metrics_file,
        indent=2,
    )

spark.stop()
