import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import avg, col

from project_config import create_spark_session, data_path, reports_path

spark = create_spark_session("NYC_Taxi_Visualization")

predictions = spark.read.parquet(data_path("predictions"))

# Aggregate in Spark and only collect the small chart-ready result.
borough_avg = (
    predictions.filter(col("Borough").isNotNull())
    .groupBy("Borough")
    .agg(avg("demand").alias("average_demand"))
    .orderBy(col("average_demand").desc())
    .toPandas()
)
plt.figure(figsize=(10, 5))
sns.barplot(data=borough_avg, x="Borough", y="average_demand")
plt.title("Borough Bazlı Ortalama Saatlik Talep")
plt.ylabel("Ortalama Talep")
plt.xlabel("Borough")
plt.tight_layout()
plt.savefig(reports_path("borough_demand.png"), dpi=150)
plt.close()

# Collect exactly one week for the example zone instead of the complete test set.
sample_zone = (
    predictions.filter(col("Zone") == "Midtown Center")
    .select("hour_start", "demand", "prediction")
    .orderBy("hour_start")
    .limit(168)
    .toPandas()
)
plt.figure(figsize=(14, 5))
plt.plot(sample_zone["hour_start"], sample_zone["demand"], label="Gerçek", alpha=0.8)
plt.plot(sample_zone["hour_start"], sample_zone["prediction"], label="Tahmin", alpha=0.8)
plt.title("Midtown Center — Gerçek vs Tahmin (1 Hafta)")
plt.legend()
plt.tight_layout()
plt.savefig(reports_path("prediction_vs_actual.png"), dpi=150)
plt.close()

print("Görseller kaydedildi.")
spark.stop()
