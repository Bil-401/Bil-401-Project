from pyspark.sql.functions import (
    col, hour, dayofweek, count, avg,
    lag, window
)
from pyspark.sql.window import Window

from project_config import create_spark_session, data_path

spark = create_spark_session("NYC_Taxi_Features")

df = spark.read.parquet(data_path("processed", "taxi_clean"))

# Saatlik talep aggregation (bu MapReduce wide shuffle'ı tetikler)
df_hourly = df.groupBy(
    window(col("tpep_pickup_datetime"), "1 hour"),
    col("PULocationID"),
    col("Borough"),
    col("Zone")
).agg(
    count("*").alias("demand")
).select(
    col("window.start").alias("hour_start"),
    col("PULocationID"),
    col("Borough"),
    col("Zone"),
    col("demand")
)

# Window fonksiyonu: zone bazında lag ve rolling average
w = Window.partitionBy("PULocationID").orderBy("hour_start")

df_features = df_hourly \
    .withColumn("hour_of_day", hour(col("hour_start"))) \
    .withColumn("day_of_week", dayofweek(col("hour_start"))) \
    .withColumn("lag_1h", lag("demand", 1).over(w)) \
    .withColumn("lag_24h", lag("demand", 24).over(w)) \
    .withColumn("rolling_avg_3h",
        avg("demand").over(w.rowsBetween(-3, -1))
    ) \
    .na.drop()  # lag'dan kaynaklanan null'ları temizle

# Feature store'a kaydet
df_features.write \
    .partitionBy("PULocationID") \
    .mode("overwrite") \
    .parquet(data_path("feature_store"))

print(f"Feature store oluşturuldu. Satır sayısı: {df_features.count():,}")
spark.stop()
