from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, lag, avg
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
    .appName("NYC_Taxi_Inference") \
    .config("spark.driver.memory", "8g") \
    .master("local[*]") \
    .getOrCreate()

# Eğitilmiş modeli yükle
model = PipelineModel.load("data/model/gbt_taxi_demand")

# Feature store'dan özellikleri oku (yeniden hesaplamaya gerek yok)
features = spark.read.parquet("data/feature_store/")

# Örnek: Mart 2019 verisi üzerinde batch inference
inference_data = features.filter(
    col("hour_start") >= "2019-03-01"
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
).write.mode("overwrite").parquet("data/predictions/")

# Özet metrikler yazdır
from pyspark.ml.evaluation import RegressionEvaluator

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

print(f"Inference tamamlandı.")
print(f"RMSE : {rmse:.2f}")
print(f"R²   : {r2:.4f}")
print(f"Tahmin edilen kayıt sayısı: {predictions.count():,}")