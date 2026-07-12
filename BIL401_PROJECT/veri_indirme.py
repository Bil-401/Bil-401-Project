# download_data.py
import urllib.request
import os

os.makedirs("data/raw", exist_ok=True)

# 2019 yılı - önce sadece Ocak-Mart ile başla (test için)
months = ["01", "02", "03"]
for m in months:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-{m}.parquet"
    dest = f"data/raw/yellow_tripdata_2019-{m}.parquet"
    print(f"İndiriliyor: {m}...")
    urllib.request.urlretrieve(url, dest)
    print(f"Tamamlandı: {dest}")

# Taxi Zone Lookup
urllib.request.urlretrieve(
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    "data/raw/taxi_zone_lookup.csv"
)
print("Zone lookup indirildi.")