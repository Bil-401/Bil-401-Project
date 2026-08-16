import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import avg, col, count, month

from project_config import create_spark_session, data_path, reports_path


spark = create_spark_session("NYC_Taxi_Visualization")

# Aralık 2019 test/batch-inference sonuçları.
predictions = spark.read.parquet(data_path("predictions"))

# Tam yıl boyunca oluşturulmuş bölge-saat özellikleri.
features = spark.read.parquet(data_path("feature_store"))

sns.set_theme(style="whitegrid")


# 1) Tam yıl kapsamını görünür kılan aylık ortalama bölge-saat talebi.
monthly_demand = (
    features.groupBy(month("hour_start").alias("month"))
    .agg(avg("demand").alias("average_demand"))
    .orderBy("month")
    .toPandas()
)

month_names = {
    1: "Ocak",
    2: "Şubat",
    3: "Mart",
    4: "Nisan",
    5: "Mayıs",
    6: "Haziran",
    7: "Temmuz",
    8: "Ağustos",
    9: "Eylül",
    10: "Ekim",
    11: "Kasım",
    12: "Aralık",
}
monthly_demand["Ay"] = monthly_demand["month"].map(month_names)

plt.figure(figsize=(12, 5))
sns.lineplot(
    data=monthly_demand,
    x="Ay",
    y="average_demand",
    marker="o",
    linewidth=2.2,
    color="#2f6f9f",
)
plt.title("2019 Aylık Ortalama Bölge-Saat Taksi Talebi")
plt.xlabel("Ay")
plt.ylabel("Ortalama Talep")
plt.xticks(rotation=35, ha="right")
plt.tight_layout()
plt.savefig(reports_path("monthly_demand_2019.png"), dpi=150)
plt.close()


# 2) Yalnızca Aralık test dönemindeki gerçek taleplerin borough ortalamaları.
borough_avg = (
    predictions.filter(col("Borough").isNotNull())
    .groupBy("Borough")
    .agg(avg("demand").alias("average_demand"))
    .orderBy(col("average_demand").desc())
    .toPandas()
)

plt.figure(figsize=(10, 5))
sns.barplot(
    data=borough_avg,
    x="Borough",
    y="average_demand",
    color="#3b7ca6",
)
plt.title("Aralık 2019 Test Dönemi - Borough Bazlı Ortalama Saatlik Talep")
plt.ylabel("Ortalama Talep")
plt.xlabel("Borough")
plt.xticks(rotation=25, ha="right")
plt.tight_layout()
plt.savefig(reports_path("borough_demand.png"), dpi=150)
plt.close()


# 3) Status raporda planlanan zone bazlı hata analizi.
# Çok az kaydı olan bölgelerin birkaç uç değerle listenin başına çıkmasını önlemek
# için Aralık ayında en az 100 bölge-saat gözlemi bulunan zone'lar kullanılır.
zone_errors = (
    predictions.filter(col("Zone").isNotNull())
    .withColumn("absolute_error", spark_abs(col("demand") - col("prediction")))
    .groupBy("Zone")
    .agg(
        avg("absolute_error").alias("mae"),
        count("*").alias("records"),
    )
    .filter(col("records") >= 100)
    .orderBy(col("mae").desc())
    .limit(15)
    .toPandas()
    .sort_values("mae", ascending=True)
)

plt.figure(figsize=(10, 7))
sns.barplot(data=zone_errors, x="mae", y="Zone", color="#e07a5f")
plt.title("Aralık 2019 - En Yüksek MAE'ye Sahip 15 Taksi Bölgesi")
plt.xlabel("Ortalama Mutlak Hata (MAE)")
plt.ylabel("Taksi Bölgesi")
plt.tight_layout()
plt.savefig(reports_path("zone_error_analysis.png"), dpi=150)
plt.close()


# 4) Görülmemiş Aralık test döneminden Midtown Center için ilk hafta.
sample_zone = (
    predictions.filter(col("Zone") == "Midtown Center")
    .select("hour_start", "demand", "prediction")
    .orderBy("hour_start")
    .limit(168)
    .toPandas()
)

plt.figure(figsize=(14, 5))
plt.plot(
    sample_zone["hour_start"],
    sample_zone["demand"],
    label="Gerçek",
    alpha=0.85,
)
plt.plot(
    sample_zone["hour_start"],
    sample_zone["prediction"],
    label="Tahmin",
    alpha=0.85,
)
plt.title("Midtown Center - Gerçek ve Tahmin (Aralık 2019, İlk Hafta)")
plt.xlabel("Tarih")
plt.ylabel("Saatlik Talep")
plt.legend()
plt.tight_layout()
plt.savefig(reports_path("prediction_vs_actual.png"), dpi=150)
plt.close()

print("Görseller kaydedildi:")
print("- monthly_demand_2019.png")
print("- borough_demand.png")
print("- zone_error_analysis.png")
print("- prediction_vs_actual.png")

spark.stop()
