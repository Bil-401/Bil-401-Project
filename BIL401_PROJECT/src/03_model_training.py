from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder \
    .appName("NYC_Taxi_GBT") \
    .config("spark.driver.memory", "8g") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.parquet("data/feature_store/")

# Feature vektörü oluştur
feature_cols = ["hour_of_day", "day_of_week", "lag_1h", "lag_24h", "rolling_avg_3h"]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

# Train/test split (kronolojik - shuffle etme!)
train = df.filter(col("hour_start") < "2019-10-01")
test  = df.filter(col("hour_start") >= "2019-10-01")

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

print(f"RMSE: {evaluator_rmse.evaluate(predictions):.2f}")
print(f"R²  : {evaluator_r2.evaluate(predictions):.4f}")

# Modeli kaydet
model.save("data/model/gbt_taxi_demand")