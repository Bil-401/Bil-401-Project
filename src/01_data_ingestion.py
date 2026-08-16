from pyspark.sql.functions import col, year

from project_config import DATA_YEAR, create_spark_session, data_path

spark = create_spark_session("NYC_Taxi_Ingestion")

# Ham veriyi oku
df = spark.read.parquet(data_path("raw", f"yellow_tripdata_{DATA_YEAR}-*.parquet"))

print(f"Toplam satır: {df.count():,}")
print(f"Kolonlar: {df.columns}")
df.printSchema()

# Temel temizlik
df_clean = df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("PULocationID").isNotNull()) &
    (year(col("tpep_pickup_datetime")) == DATA_YEAR)
)

# Zone lookup join
# eşleme bulunamayan yolculukların tamamen kaybolmasını önler ve veri kalitesi sorununu Unknown veya N/A kategorileri üzerinden gözlenebilir kılar.
zone_lookup = spark.read.csv(
    data_path("raw", "taxi_zone_lookup.csv"),
    header=True, 
    inferSchema=True
)

df_joined = df_clean.join(
    zone_lookup.select("LocationID", "Borough", "Zone"),
    df_clean.PULocationID == zone_lookup.LocationID,
    "left"
)

# Temiz veriyi kaydet
df_joined.write.mode("overwrite").parquet(data_path("processed", "taxi_clean"))
print("Temiz veri kaydedildi.")
spark.stop()
