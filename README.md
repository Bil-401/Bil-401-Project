# BIL401 - Distributed Taxi Demand Forecasting

This project implements an end-to-end, PySpark-based machine-learning data pipeline for hourly NYC Yellow Taxi demand forecasting at taxi-zone level. The validated experiment uses January-March 2019: 22,612,607 raw trip records are cleaned and aggregated into a 290,928-row feature store. A Spark MLlib Gradient-Boosted Tree regressor achieved RMSE 27.79 and R² 0.9610 on the chronological March test split.

## Pipeline

1. `01_data_ingestion.py`: reads NYC TLC Parquet files, applies quality filters, joins taxi-zone metadata, and writes cleaned Parquet.
2. `02_feature_engineering.py`: performs hourly `groupBy`, window-based lag/rolling features, and writes a `PULocationID`-partitioned feature store.
3. `03_model_training.py`: trains the MLlib GBT model on January-February, evaluates it on March, compares it with a lag-24h persistence baseline, and saves metrics.
4. `04_inference.py`: reloads the saved pipeline model, performs March batch inference, and saves predictions and metrics.
5. `05_visualization.py`: aggregates in Spark and collects only chart-sized outputs into Pandas.

## Repository layout

```text
401/
├── 401-project/
│   ├── run_all.py
│   ├── veri_indirme.py
│   ├── reports/
│   └── data/                 # generated locally; not included
├── src/
│   ├── project_config.py
│   └── 01_...py through 05_...py
├── hadoop/bin/               # optional Windows compatibility binaries
├── requirements.txt
└── DEMO_GUIDE.md
```

## Data sources

- NYC TLC Yellow Taxi Trip Records (2019 monthly Parquet): `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`
- Direct Parquet URL pattern: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-MM.parquet`
- Taxi Zone Lookup: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

The raw data is public but intentionally not included in the submission because of its size. The downloader recreates the exact three-month experiment.

## Requirements

- Python 3.10 or 3.11
- Java JDK 17 (`JAVA_HOME` must point to the JDK)
- Approximately 25 GB free disk space for download, intermediate Parquet, model, and predictions
- 16 GB RAM recommended; the scripts default to an 8 GB Spark driver

Create and activate a virtual environment, then run from the `401-project` directory:

```bash
python -m pip install -r ../requirements.txt
python run_all.py --download
```

If the data is already under `401-project/data/raw/`, omit `--download`:

```bash
python run_all.py
```

To choose months explicitly:

```bash
python run_all.py --download --months 01,02,03
```

Environment overrides:

- `SPARK_DRIVER_MEMORY` (default `8g`)
- `SPARK_MASTER` (default `local[*]`)
- `JAVA_HOME` (must be configured by the user/system)
- `HADOOP_HOME` (optional on Windows; an existing value is respected)

## Generated outputs

The pipeline creates these paths under `401-project/`:

- `data/processed/taxi_clean/`
- `data/feature_store/`
- `data/model/gbt_taxi_demand/`
- `data/predictions/`
- `reports/training_metrics.json`
- `reports/inference_metrics.json`
- `reports/borough_demand.png`
- `reports/prediction_vs_actual.png`

## Reproducibility and interpretation

The split is chronological: records before 1 March 2019 are used for training and March is used for testing and demonstration inference. The reported inference and test metrics therefore match by design; March is not a second unseen holdout. The model uses `hour_of_day`, `day_of_week`, `lag_1h`, `lag_24h`, and `rolling_avg_3h`. The training script now records a lag-24h persistence baseline so that the GBT result is not interpreted from R² alone.

The current validated scope is three months, not the full 2019 year. Expanding the month list does not require an architectural change, but it increases download, shuffle, disk, and memory costs. For a strict unseen production evaluation, a future run should reserve a fourth month for inference after training and model-selection periods.

## Troubleshooting

- `JAVA_GATEWAY_EXITED`: verify `java -version` and `JAVA_HOME` (JDK 17).
- Windows Hadoop file-system errors: set `HADOOP_HOME` to the included `hadoop` directory or a trusted compatible installation.
- Out-of-memory during shuffle: lower the month count or increase `SPARK_DRIVER_MEMORY`; close other applications.
- Existing-output conflicts are avoided because Spark writes generated datasets and the model with overwrite mode.

See `DEMO_GUIDE.md` for the 20-minute presentation plan.
