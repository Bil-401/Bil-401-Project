import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from pyspark.sql import SparkSession

os.makedirs("reports", exist_ok=True)

spark = SparkSession.builder.appName("Viz").master("local[*]").getOrCreate()

predictions = spark.read.parquet("data/predictions/")

# Spark → Pandas (görselleştirme için küçük sample al)
pdf = predictions.select(
    "hour_start", "PULocationID", "Borough", "Zone", "demand", "prediction"
).toPandas()

# 1. Borough bazlı ortalama talep
borough_avg = pdf.groupby("Borough")["demand"].mean().sort_values(ascending=False)
plt.figure(figsize=(10, 5))
sns.barplot(x=borough_avg.index, y=borough_avg.values)
plt.title("Borough Bazlı Ortalama Saatlik Talep")
plt.ylabel("Ortalama Talep")
plt.savefig("reports/borough_demand.png", dpi=150)
plt.close()

# 2. Gerçek vs Tahmin (örnek zone)
sample_zone = pdf[pdf["Zone"] == "Midtown Center"].sort_values("hour_start").head(168)
plt.figure(figsize=(14, 5))
plt.plot(sample_zone["hour_start"], sample_zone["demand"], label="Gerçek", alpha=0.8)
plt.plot(sample_zone["hour_start"], sample_zone["prediction"], label="Tahmin", alpha=0.8)
plt.title("Midtown Center — Gerçek vs Tahmin (1 Hafta)")
plt.legend()
plt.savefig("reports/prediction_vs_actual.png", dpi=150)
plt.close()

print("Görseller kaydedildi.")