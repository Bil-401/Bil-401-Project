from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder \
    .appName("NYC_Taxi_Ingestion") \
    .config("spark.driver.memory", "8g") \
    .master("local[*]") \
    .getOrCreate()

# Ham veriyi oku
df = spark.read.parquet("data/raw/yellow_tripdata_2019-*.parquet")

print(f"Toplam satır: {df.count():,}")
print(f"Kolonlar: {df.columns}")
df.printSchema()

# Temel temizlik
df_clean = df.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("PULocationID").isNotNull()) &
    (year(col("tpep_pickup_datetime")) == 2019)
)

# Zone lookup join
zone_lookup = spark.read.csv(
    "data/raw/taxi_zone_lookup.csv", 
    header=True, 
    inferSchema=True
)

df_joined = df_clean.join(
    zone_lookup.select("LocationID", "Borough", "Zone"),
    df_clean.PULocationID == zone_lookup.LocationID,
    "left"
)

# Temiz veriyi kaydet
df_joined.write.mode("overwrite").parquet("data/processed/taxi_clean")
print("Temiz veri kaydedildi.")