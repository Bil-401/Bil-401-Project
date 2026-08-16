# Dağıtık Taksi Talep Tahmini

Bu proje, New York City Yellow Taxi yolculuk kayıtlarından taksi bölgesi bazında saatlik talep tahmini üreten uçtan uca bir PySpark veri hattıdır. Final deneyinde 2019 yılının 12 aylık Parquet dosyaları kullanılmıştır. Eğitim dönemi Ocak-Kasım, kronolojik test ve batch inference dönemi ise Aralık 2019'dur.

## Doğrulanmış final sonuçları

| Ölçüm | Sonuç |
|---|---:|
| Ham yolculuk kaydı | 84.598.444 |
| Feature store satırı | 1.080.759 |
| Aralık tahmin kaydı | 83.319 |
| GBT RMSE | 27,71 |
| GBT R² | 0,9594 |
| Lag-24h baseline RMSE | 49,48 |
| Lag-24h baseline R² | 0,8704 |
| Baseline'a göre RMSE azalması | %44,0 |

Sonuçlar `training_metrics.json`, `inference_metrics.json` ve tam yıl pipeline konsol çıktısıyla doğrulanmıştır.

## Veri hattı

1. `01_data_ingestion.py`: Aylık NYC TLC Parquet dosyalarını okur, veri kalitesi filtrelerini uygular, taksi bölgesi bilgileriyle birleştirir ve temiz Parquet çıktısı üretir.
2. `02_feature_engineering.py`: Bölge-saat talebini hesaplar; saat, gün, lag-1, lag-24 ve üç gözlemlik kayan ortalama özelliklerini üretir.
3. `03_model_training.py`: Ocak-Kasım verisiyle MLlib GBT modelini eğitir, Aralık verisinde değerlendirir ve lag-24 persistence baseline ile karşılaştırır.
4. `04_inference.py`: Kaydedilmiş modeli yükler, Aralık verisinde batch inference gerçekleştirir ve metrikleri kaydeder.
5. `05_visualization.py`: Büyük agregasyonları Spark üzerinde yapar; yalnızca grafik boyutundaki sonuçları Pandas'a aktarır.

## Klasör yapısı

```text
Bil-401-Project/
|-- 401-project/
|   |-- data/
|   |   |-- raw/
|   |   |-- processed/
|   |   |-- feature_store/
|   |   |-- model/
|   |   `-- predictions/
|   |-- reports/
|   |-- run_all.py
|   `-- veri_indirme.py
|-- src/
|   |-- 01_data_ingestion.py
|   |-- 02_feature_engineering.py
|   |-- 03_model_training.py
|   |-- 04_inference.py
|   |-- 05_visualization.py
|   `-- project_config.py
|-- hadoop/bin/
|-- README.md
|-- DEMO_GUIDE.md
`-- requirements.txt
```

## Veri kaynakları

- NYC TLC Yellow Taxi Trip Records: `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- Aylık Parquet adres şablonu: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-AA.parquet`
- Taksi bölgesi eşleme dosyası: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

Ham veri, dosya boyutu nedeniyle teslim paketine eklenmez. İndirme betiği mevcut ve dolu dosyaları atlar; bu nedenle kesilen işlem yeniden başlatılabilir.

## Ortam gereksinimleri

- Python 3.10 veya 3.11
- JDK 17
- PySpark 3.5.1
- Yeterli boş disk alanı
- Windows'ta gerekirse proje ile verilen `hadoop/bin` uyumluluk dosyaları

Kurulum:

```bash
python -m pip install -r requirements.txt
```

Windows'ta `JAVA_HOME`, JDK 17 ana klasörünü göstermelidir; `bin` klasörünü veya JDK 22 kurulumunu göstermemelidir.

Örnek Anaconda Prompt ayarı:

```bat
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
set "HADOOP_HOME=C:\Bil-401-Project\hadoop"
set "PATH=%HADOOP_HOME%\bin;%JAVA_HOME%\bin;%PATH%"
mkdir C:\spark-temp
set "TEMP=C:\spark-temp"
set "TMP=C:\spark-temp"
set "SPARK_LOCAL_DIRS=C:\spark-temp"
```

`NativeIO$Windows.access0` hatası alınırsa `hadoop.dll` ve `winutils.exe` dosyalarının PySpark ile gelen Hadoop 3.3.x sürümüyle uyumlu olduğu doğrulanmalıdır. Kullanıcı dizininde Türkçe karakter bulunuyorsa ASCII karakterli `C:\spark-temp` klasörü geçici Spark yolu olarak kullanılmalıdır.

## Tam yıl verisini indirme

`401-project` klasöründe:

```bash
python veri_indirme.py --year 2019 --months 01,02,03,04,05,06,07,08,09,10,11,12
```

Ocak-Mart dosyaları zaten varsa betik bunları tekrar indirmez ve yalnızca eksik ayları tamamlar.

## Çalıştırma

Veriler hazırsa:

```bash
python run_all.py
```

Veriyi indirip ardından hattı çalıştırmak için:

```bash
python run_all.py --download
```

Görselleştirmeyi atlamak için:

```bash
python run_all.py --skip-visualization
```

## Deneysel ayrım

- Eğitim: `hour_start < 2019-12-01` (Ocak-Kasım 2019)
- Test: `2019-12-01 <= hour_start < 2020-01-01` (Aralık 2019)
- Batch inference: Aralık 2019

Test ve inference aynı Aralık kayıtlarını kullandığı için iki aşamadaki GBT metriklerinin aynı olması beklenir. Aralık ikinci bir bağımsız inference holdout'u değil, kronolojik final test dönemidir.

## Üretilen çıktılar

- `data/processed/taxi_clean/`
- `data/feature_store/`
- `data/model/gbt_taxi_demand/`
- `data/predictions/`
- `reports/training_metrics.json`
- `reports/inference_metrics.json`
- `reports/borough_demand.png`
- `reports/prediction_vs_actual.png`

`training_metrics.json`, GBT ve lag-24 baseline için RMSE ve R² değerlerini içerir. `inference_metrics.json`, Aralık tahmin sayısını ve batch-inference metriklerini içerir. Bu final çalıştırmanın sonuçları rapora, sunuma ve demo konuşma metnine işlenmiştir.

## Disk kullanımı

NYC TLC'nin aylık dosyaları sıkıştırılmış Parquet olduğu için indirme boyutu, satırların bellekte veya CSV biçiminde kaplayacağı alandan çok daha küçüktür. En yüksek disk kullanımı ham indirmeden ziyade temiz Parquet, feature store, prediction çıktıları ve Spark geçici shuffle dosyaları birlikte oluştuğunda görülebilir.

Windows PowerShell ile veri klasörünün gerçek boyutu şöyle ölçülebilir:

```powershell
(Get-ChildItem "C:\Bil-401-Project\401-project\data" -Recurse -File |
    Measure-Object Length -Sum).Sum / 1GB
```

## Bilinen sınırlılıklar

- Uygulama `local[*]` modunda tek bilgisayarın çekirdeklerini kullanır; çok düğümlü cluster deneyi yapılmaz.
- Lag özellikleri satır ofsetiyle hesaplanır. Eksik bölge-saat kaydı varsa lag-24 tam olarak 24 saat öncesini temsil etmeyebilir.
- Hava durumu, tatil, etkinlik ve komşu bölge bilgileri modele dahil değildir.
- Aralık, hem kronolojik test hem batch-inference gösterim dönemidir.

## Sonuçların kaynağı ve yorum

Final raporda yalnızca son tam yıl çalıştırmasında üretilen `training_metrics.json`, `inference_metrics.json`, konsol satır sayıları ve grafikler kullanılmıştır. Önceki üç aylık çalışmanın metrikleri tam yıl sonucu olarak sunulmamıştır. GBT, lag-24 baseline'a kıyasla RMSE'yi yaklaşık %44 azaltmış; Midtown Center grafiğinde günlük talep döngüsünü güçlü biçimde izlemiş, ancak bazı keskin zirveleri yumuşatmıştır.
