# 20 Dakikalık Türkçe Demo Konuşma Metni

## Konuşmacılar ve görev dağılımı

- **Zeynep Ay:** Açılış, problem, veri seti, klasör yapısı, veri indirme, orkestrasyon ve ingestion.
- **Sümeyye Sıla Altay:** Özellik mühendisliği, model eğitimi, baseline, inference, görselleştirme, sonuçlar ve sınırlılıklar.

İki konuşmacının hedef süresi yaklaşık 10'ar dakikadır. Demo öncesinde `training_metrics.json`, `inference_metrics.json`, iki PNG grafik ve final rapor açık tutulmalıdır. Uzun süren tam pipeline sunum sırasında yeniden çalıştırılmamalıdır.

---

## 0:00-1:00 - Açılış

**Konuşmacı: Zeynep**  
**Ekranda:** Final raporun kapak sayfası

“Merhaba, biz Zeynep Ay ve Sümeyye Sıla Altay. Projemizin adı Dağıtık Taksi Talep Tahmini. Bu projede New York City Yellow Taxi yolculuk kayıtlarını kullanarak her taksi bölgesi için saatlik talebi tahmin eden, Apache Spark üzerinde çalışan uçtan uca bir veri hattı geliştirdik. Sunumda önce veri ve mimariyi, ardından özellik mühendisliği, modelleme, çıkarım ve sonuçları göstereceğiz.”

## 1:00-3:00 - Problem ve tam yıl veri kapsamı

**Konuşmacı: Zeynep**  
**Ekranda:** `README.md` dosyasının giriş ve veri kaynakları bölümleri

“Tahmin hedefimiz, belirli bir taksi bölgesinde belirli bir saatte gerçekleşecek pickup sayısıdır. Final deneyinde 2019 yılının 12 aylık Yellow Taxi Parquet dosyalarını kullandık. Veri sıkıştırılmış Parquet biçiminde tutulduğu için indirme boyutu satırların bellekte kapladığı alandan daha küçüktür. Buna rağmen ingestion, shuffle, feature store ve prediction çıktıları nedeniyle çalışma klasik küçük veri analizinden farklı bir ölçek sunmaktadır.”

“Deneysel ayrımı kronolojik yaptık. Ocak-Kasım verilerini eğitim, Aralık 2019 verisini test ve batch inference dönemi olarak kullandık. Böylece gelecekteki Aralık kayıtlarını geçmiş ayların modelini değerlendirmek için ayırdık.”

“Tam yıl çalıştırmasında toplam 84 milyon 598 bin 444 ham kayıt okundu ve özellik mühendisliği sonunda 1 milyon 80 bin 759 bölge-saat kaydı üretildi. Bu sayılar, ara rapordaki üç aylık doğrulamadan farklı olarak final deneyinin gerçekten 12 ay üzerinde yürütüldüğünü gösteriyor.”

## 3:00-5:00 - Klasör yapısı ve veri indirme

**Konuşmacı: Zeynep**  
**Ekranda:** VS Code Explorer, `veri_indirme.py`, ardından `README.md`

“Proje klasöründe çalıştırma dosyaları `401-project`, işlem adımları ise `src` klasöründedir. Ham ve üretilen büyük veriler `401-project/data` altında tutulur. Raporlanabilir metrik ve grafikler `401-project/reports` klasörüne yazılır.”

“`veri_indirme.py` yıl ve ay listesini parametre olarak alır. Mevcut ve dolu dosyaları tekrar indirmez. İndirme sırasında önce `.part` uzantılı geçici dosya oluşturduğu için yarım kalan bir indirme geçerli Parquet dosyası sanılmaz. Varsayılan ay listesi artık 2019 yılının 12 ayıdır.”

## 5:00-7:00 - Orkestrasyon ve veri alımı

**Konuşmacı: Zeynep**  
**Ekranda:** `run_all.py`, `project_config.py`, `01_data_ingestion.py`

“`run_all.py` beş aşamayı sırasıyla ayrı Python süreçlerinde çalıştırır. Bir aşama başarısız olursa sonraki aşamaya geçmez; buna fail-fast davranışı diyoruz. Makineye özel sabit yollar kullanmak yerine dosya konumları `pathlib` ile çözülür. Ortak Spark ayarları ve tam yıl tarih sınırları `project_config.py` içinde tutulur.”

“İlk aşama olan `01_data_ingestion.py`, 2019'a ait bütün aylık Parquet dosyalarını tek Spark DataFrame olarak okur. Geçersiz yolcu, mesafe, ücret, bölge ve tarih kayıtlarını filtreler. Ardından taxi zone lookup dosyasını kullanarak Borough ve Zone bilgilerini ekler ve sonucu temiz Parquet olarak kaydeder.”

## 7:00-10:00 - Özellik mühendisliği

**Konuşmacı: Sümeyye**  
**Ekranda:** `02_feature_engineering.py`

“İkinci aşamada yolculukları bir saatlik pencere, PULocationID, Borough ve Zone alanlarına göre grupluyoruz. Her gruptaki kayıt sayısı demand, yani tahmin edeceğimiz saatlik talep oluyor. Bu groupBy işlemi aynı anahtara ait kayıtların aynı partition'a taşınmasını gerektirdiği için wide transformation ve shuffle oluşturur.”

“Daha sonra her taksi bölgesi içinde zaman sırasına göre Spark Window tanımlıyoruz. Model özelliklerimiz saat, haftanın günü, bir önceki gözlem, 24 gözlem önceki talep ve önceki üç gözlemin ortalamasıdır. Oluşan feature store'u PULocationID'ye göre partition edilmiş Parquet olarak kaydediyoruz. Böylece eğitim ve çıkarım aynı özellik katmanını tekrar hesaplamadan kullanabiliyor.”

“Buradaki önemli sınırlılık, lag değerlerinin satır ofsetine dayanmasıdır. Eksik bir bölge-saat kaydı varsa 24 gözlem önceki değer tam olarak 24 saat öncesine karşılık gelmeyebilir. Bunu raporda açıkça belirttik.”

## 10:00-13:00 - Model eğitimi ve baseline

**Konuşmacı: Sümeyye**  
**Ekranda:** `03_model_training.py`, ardından `training_metrics.json`

“Modelleme aşamasında beş özelliği VectorAssembler ile tek bir özellik vektöründe birleştiriyoruz. Ardından Spark MLlib GBTRegressor modelini 50 iterasyon ve maksimum derinlik 5 ayarıyla eğitiyoruz. Rastgele train-test ayrımı kullanmıyoruz; Ocak-Kasım eğitim, Aralık test dönemidir.”

“GBT sonucunu yalnız başına yorumlamamak için lag-24 persistence baseline da hesaplıyoruz. Bu baseline, bir bölgenin talebinin 24 gözlem önceki değerle aynı olacağını varsayar. Hem GBT hem baseline için RMSE ve R² değerlerini aynı Aralık test kümesinde hesaplayıp `training_metrics.json` dosyasına yazıyoruz.”

“GBT modeli 27 virgül 71 RMSE ve 0 virgül 9594 R-kare elde etti. Lag-24 baseline ise 49 virgül 48 RMSE ve 0 virgül 8704 R-kare üretti. Böylece GBT, baseline'a göre RMSE'yi yaklaşık yüzde 44 azalttı. Bu karşılaştırma, modelin yalnızca bir önceki günün değerini tekrar etmekten daha güçlü olduğunu gösteriyor.”

## 13:00-15:00 - Batch inference

**Konuşmacı: Sümeyye**  
**Ekranda:** `04_inference.py`, `inference_metrics.json`

“`04_inference.py`, daha önce kaydedilen MLlib Pipeline modelini diskten geri yükler. Feature store içindeki Aralık kayıtlarında batch inference yapar ve tahminleri Parquet biçiminde kaydeder. Pipeline modelini kaydettiğimiz için hem VectorAssembler hem de GBT modeli birlikte geri yüklenir.”

“Test ve inference aynı Aralık kayıtlarını kullandığı için buradaki RMSE ve R² değerlerinin eğitim betiğindeki test değerleriyle aynı olması beklenir. Aralık ayrı bir ikinci holdout değil, final kronolojik test ve gösterim dönemidir.”

“Aralık döneminde toplam 83 bin 319 bölge-saat kaydı için tahmin üretildi. İnference aşamasındaki RMSE ve R-kare değerleri eğitim betiğindeki Aralık test sonuçlarıyla aynı çıktı; bu da kaydedilen modelin doğru biçimde yüklenip aynı veri üzerinde çalıştığını doğruluyor.”

## 15:00-17:30 - Görselleştirme ve sonuçlar

**Konuşmacı: Sümeyye**  
**Ekranda:** `05_visualization.py`, `borough_demand.png`, `prediction_vs_actual.png`

“Görselleştirme aşamasında tüm prediction tablosunu Pandas'a taşımıyoruz. Borough ortalamalarını önce Spark üzerinde hesaplıyor, Pandas'a yalnızca grafik için gereken birkaç satırı aktarıyoruz. Zaman serisi grafiği için de Midtown Center bölgesinin Aralık ayındaki ilk 168 gözlemini, yani yaklaşık bir haftalık bölümü kullanıyoruz.”

“İlk grafikte Manhattan açık ara en yüksek ortalama saatlik talebe sahip. `Unknown` kategorisinin ikinci sırada görünmesi, bölge eşlemesi eksik veya standart borough sınıflarının dışında kalan kayıtlar için ayrıca veri kalitesi kontrolü gerektiğini gösteriyor. Queens üçüncü sırada; diğer borough'ların ortalaması belirgin biçimde daha düşük.”

“İkinci grafikte Midtown Center için gerçek ve tahmin eğrileri günlük dip ve yükseliş döngülerinde birbirini yakından izliyor. Model genel örüntüyü iyi yakalıyor; ancak yaklaşık 900 ile 950 arasındaki bazı keskin gerçek talep zirvelerini daha düşük ve daha yumuşak tahmin ediyor. Dolayısıyla modelin genel başarısı yüksek olsa da ani yoğunluk zirvelerinde geliştirme alanı bulunuyor.”

## 17:30-19:00 - Büyük veri boyutu ve sınırlılıklar

**Konuşmacı: Zeynep**  
**Ekranda:** Final raporun “Büyük Veri ve Dağıtık İşleme Boyutu” ile “Sınırlılıklar” bölümleri

“Projenin büyük veri boyutu yalnızca satır sayısından gelmiyor. Çoklu Parquet okuma, groupBy shuffle, bölge bazlı Window sıralaması, partition edilmiş feature store ve MLlib eğitimi Spark üzerinde yürütülüyor. Uygulama şu an `local[*]` modunda tek bilgisayarın çekirdeklerini kullanıyor; çok düğümlü cluster performansı ölçülmedi.”

“Diğer sınırlılıklarımız eksik bölge-saat kayıtları, hava ve tatil gibi dışsal özelliklerin kullanılmaması ve Aralık ayının hem final test hem batch-inference gösterim dönemi olmasıdır. Gelecek çalışmada tam bölge-saat ızgarası, dışsal özellikler ve ayrı validation dönemi eklenebilir.”

## 19:00-20:00 - Kapanış

**Konuşmacı: Zeynep ve Sümeyye**  
**Ekranda:** Mimari şekli veya final sonuç tablosu

**Zeynep:** “Özetle, 2019 yılının tamamını işleyebilen, ham veriden tahmin ve görselleştirmeye kadar beş aşamalı, taşınabilir ve tekrarlanabilir bir Spark veri hattı geliştirdik.”

**Sümeyye:** “Modeli kronolojik Aralık test döneminde değerlendirdik, lag-24 baseline ile karşılaştırdık ve bütün metrikleri yeniden üretilebilir JSON çıktıları olarak kaydettik. Dinlediğiniz için teşekkür ederiz, sorularınızı alabiliriz.”

---

## Demo öncesi kontrol listesi

1. Tam yıl pipeline'ını sunumdan önce bir kez çalıştırın.
2. `training_metrics.json` ve `inference_metrics.json` dosyalarının oluştuğunu doğrulayın.
3. `borough_demand.png` ve `prediction_vs_actual.png` dosyalarını açın.
4. Sunumdaki sayıların `training_metrics.json` ve `inference_metrics.json` ile aynı olduğunu kontrol edin.
5. Final rapor ile sunumun iki güncel PNG grafiğini kullandığını doğrulayın.
6. Sunum sırasında tam pipeline'ı yeniden çalıştırmayın; hazır çıktı ve kodları gösterin.

## Sunum dosyasıyla eşleştirme

- Slayt 1-4: Zeynep - açılış, problem, veri ölçeği ve mimari.
- Slayt 5: Sümeyye - özellik mühendisliği.
- Slayt 6-9: Sümeyye - deney düzeni, metrikler ve grafikler.
- Slayt 10: Zeynep - büyük veri boyutu, sınırlılıklar ve gelecek çalışma.
- Slayt 11: Zeynep ve Sümeyye - ortak sonuç ve soru-cevap.
