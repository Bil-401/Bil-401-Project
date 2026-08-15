# 20-Minute Demo Guide

## Before the demo

1. Confirm `java -version` and activate the Python environment.
2. Keep `401-project/data/` on disk; do not download or retrain live unless explicitly requested.
3. Run `python run_all.py` once before presentation and verify that both PNG files and JSON metric files exist under `401-project/reports/`.
4. Open the final report, both output charts, and the source folder in advance.

## Suggested flow

### 0:00-2:00 - Problem and scale

- Goal: predict hourly Yellow Taxi pickup demand for each NYC taxi zone.
- Validated input: January-March 2019, 22,612,607 raw rows and 19 columns.
- Why big data: multi-file Parquet input, distributed aggregation, window operations, wide shuffle, partitioned feature storage, and MLlib training.

### 2:00-5:00 - Architecture

- Show `run_all.py` and explain fail-fast orchestration.
- Walk through ingestion -> feature engineering -> feature store -> model -> inference -> visualization.
- Emphasize that Spark remains the processing engine; Pandas receives only chart-sized aggregates.

### 5:00-9:00 - Distributed processing

- Show the hourly `groupBy` in `02_feature_engineering.py`: records with the same zone-hour key must meet in the same partition, causing wide shuffle.
- Show the zone-partitioned time window and the `lag_1h`, `lag_24h`, and trailing three-hour mean.
- Show the Parquet write partitioned by `PULocationID` and explain partition pruning/reuse.

### 9:00-12:00 - Model and evaluation

- Show the chronological split in `03_model_training.py`; there is no random shuffle.
- Explain the five model inputs and GBT settings (`maxIter=50`, `maxDepth=5`).
- Report GBT results: RMSE 27.79, R² 0.9610.
- State the interpretation caveat: lag features are strong; the updated code also evaluates a lag-24h persistence baseline on the next run.

### 12:00-15:00 - Results

- Show `borough_demand.png`: Manhattan has the highest average hourly demand.
- Show `prediction_vs_actual.png`: daily cycles are captured, but high peaks are underestimated.
- Connect peak underestimation to squared-error optimization, limited features, and the three-month scope.

### 15:00-17:00 - Reproducibility

- Show `README.md`, data-source URLs, `requirements.txt`, and the one-command run:

  ```bash
  python run_all.py --download
  ```

- Mention environment controls (`JAVA_HOME`, `SPARK_DRIVER_MEMORY`, `SPARK_MASTER`).

### 17:00-19:00 - Limitations and next steps

- The validated run is January-March, not full-year.
- March is both the test and batch-inference demonstration period; it is not an additional holdout.
- Future improvements: reserve an unseen fourth month, compare baseline metrics, add weather/holiday signals, fill missing zone-hour rows explicitly, and evaluate on a multi-node cluster.

### 19:00-20:00 - Conclusion and questions

- Close with the contribution: a reproducible, modular Spark pipeline that demonstrates distributed ingestion, shuffle-heavy feature engineering, partitioned Parquet storage, MLlib training, saved-model inference, and interpretable results on more than 22 million trips.

## Safe live-demo command

For a shorter live demonstration when generated data already exists, run only the final steps individually from `401-project/`:

```bash
python ../src/04_inference.py
python ../src/05_visualization.py
```

If time is tight, show the pre-generated outputs rather than running a long Spark job live.
