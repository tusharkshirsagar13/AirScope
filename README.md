# AirScope — Air Quality Intelligence

**Air Quality and Pollution Data Analytics Using Big Data Processing**
*Big Data Analysis mini project · Apache Spark (PySpark) · Parquet · Streamlit · Plotly*

**Deployment links:** [Live dashboard](https://airscope-tushar-e139.vercel.app)
· [Vercel project](https://vercel.com/tushar-e139/airscope)
· [Container deployment](https://airscope-ku9imozb1-tushar-e139.vercel.app)

Vercel Deployment Protection is enabled; access may require signing in to Vercel.

AirScope ingests an air-quality dataset with Apache Spark, cleans and validates it
transparently, stores it as partitioned Parquet, and runs every analysis — trends,
daily/weekly/seasonal profiles, location comparison, correlation, outlier detection —
as Spark aggregations. A clean Streamlit dashboard presents the results with
interactive Plotly charts, automatically generated observations, and CSV/JSON exports.

---

## 1. Problem statement

Air-quality monitoring produces long, messy time series: missing readings coded as
magic numbers, inconsistent formats, sensors that fail for months. Opening such data
in a spreadsheet hides these problems and does not scale. The goal is a reproducible
pipeline that (a) processes the data with a big-data engine, (b) is explicit about
every record it discards, and (c) turns the result into understandable pollution
patterns without fabricating anything the data does not contain.

## 2. Objectives

- Ingest CSV / Excel / JSON / Parquet data with **PySpark**, detecting delimiters,
  decimal separators and date formats automatically.
- Map source columns to known pollutants through a **configurable mapping layer**
  (no guessing), and validate every value against per-pollutant rules.
- Store cleaned data as **Parquet partitioned by year/month** and re-use it.
- Compute trends, profiles, rankings, correlations and outliers with **Spark SQL /
  DataFrame operations**, collecting only small aggregated results.
- Present results in a **simple, professional dashboard** with honest labelling of
  units, data gaps and statistical limitations.
- Provide tests, documentation, a viva Q&A and a fair **Pandas-vs-Spark benchmark**.

## 3. Key features

| Area | What you get |
|---|---|
| Ingestion | CSV (auto delimiter `, ; \t \|`, auto `,`/`.` decimals), xlsx, JSON/JSON-lines, Parquet; upload from the sidebar |
| Mapping | `config/column_mapping.yaml` — exact alias matching, ambiguity warnings, unmapped columns reported |
| Cleaning | blank rows, missing-value codes (e.g. `-200`), non-numeric text, per-pollutant valid ranges, unparseable timestamps, exact duplicates — every exclusion counted |
| Storage | partitioned Parquet + JSON cleaning report, fingerprint-based re-use, atomic writes |
| Analysis | time trends (hour/day/week/month), hour-of-day, weekday, month & season profiles, weekday vs weekend, location ranking, Pearson correlation matrix, IQR/percentile outliers, highest-pollution days, histograms, data completeness |
| Insights | deterministic, data-driven observations (no hard-coded conclusions, no causal claims) |
| Exports | CSV downloads of records and summaries; JSON report with limitations |
| Benchmark | equal-work Pandas vs Spark timing with environment details and caveats |

## 4. Technology stack

Python 3 · **Apache Spark 4.x via PySpark** (local mode) · Spark SQL · Parquet (PyArrow)
· Pandas (small results only) · NumPy · **Streamlit** · **Plotly** · PyYAML · pytest.
Spark requires a **Java runtime (JDK 17 or 21)**.

## 5. Dataset

The included `data/raw/AirQuality.csv` is the UCI **Air Quality** dataset
(Vito, S., 2008, DOI 10.24432/C59K5F, CC BY 4.0) — hourly readings from one road-level
site in an Italian city. Findings on this file:

| Property | Value |
|---|---|
| Rows in file | 9,471 (9,357 data rows + 114 blank lines) |
| Retained after cleaning | **9,326** hourly records, 10 Mar 2004 18:00 → 04 Apr 2005 14:00 |
| Columns | 15 (+2 empty trailing columns dropped): Date, Time, 5 reference pollutants, 5 sensor responses, 3 weather variables |
| Pollutants | CO (mg/m³), NMHC (µg/m³), Benzene C6H6 (µg/m³), NOx (ppb), NO2 (µg/m³) |
| Locations | 1 (no city/station column) |
| AQI / PM2.5 / PM10 / SO2 | **not present** — not invented |
| Missing values | coded `-200`; e.g. NMHC valid in only 914 rows (9.8 %) |

More detail and other-dataset instructions: `data/raw/README.md`.

## 6. Project structure

```
AirScope/
├── app.py                     Streamlit dashboard
├── requirements.txt
├── pytest.ini
├── .streamlit/config.toml     theme + upload limit
├── config/
│   ├── settings.yaml          dataset path, Spark, cleaning & analysis settings
│   └── column_mapping.yaml    column aliases, units, valid ranges
├── src/
│   ├── config.py              config loading, portable paths
│   ├── data_loader.py         Spark session, format sniffing, raw load, column mapping
│   ├── data_cleaning.py       Spark validation & cleaning + cleaning report
│   ├── spark_processor.py     ETL pipeline, partitioned Parquet, caching
│   ├── analytics.py           Spark aggregations (trends, profiles, correlation, outliers…)
│   ├── insights.py            rule-based observations
│   ├── export_utils.py        CSV / JSON report helpers
│   ├── benchmark.py           Pandas vs Spark benchmark
│   └── ui_theme.py            CSS + Plotly styling
├── tests/                     pytest suite (synthetic fixtures only)
├── data/
│   ├── raw/                   input files (never modified)
│   ├── processed/             cleaned Parquet (generated)
│   └── reports/               CSV/JSON outputs (generated)
├── docs/                      project_report.md · methodology.md · viva_questions.md
└── scripts/                   run_app.sh/.bat · run_tests.sh/.bat · run_pipeline.py · benchmark.py
```

## 7. Installation

### Prerequisites
- **Python 3.10 – 3.13** (`python3 --version`)
- **Java 17 or 21** (`java -version`)
  - macOS: `brew install openjdk@17`, then run the `sudo ln -sfn …` / PATH line Homebrew prints.
  - Windows: install *Eclipse Temurin 17* (adoptium.net) and tick "Set JAVA_HOME".

### macOS / Linux
```bash
cd AirScope
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
streamlit run app.py
```
Or simply: `bash scripts/run_app.sh` (creates `.venv` and installs on first run).

### Windows (PowerShell or Command Prompt)
```bat
cd AirScope
py -3 -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
streamlit run app.py
```
Or double-click / run `scripts\run_app.bat`.

Open **http://localhost:8501**. The first load starts Spark and cleans the data
(~15–30 s); later loads re-use the Parquet output.

## Vercel deployment

`Dockerfile.vercel` packages the dashboard with Python 3.12 and Java 17 for
Vercel's container runtime. `vercel.json` explicitly routes requests to the
dashboard container service. The container listens on `PORT` (80 by
default) and starts through `scripts/start_vercel.sh`.

The hosted configuration uses one Spark worker thread, a 768 MB JVM heap, and
a 4 MB upload limit. Local installation keeps the settings above. Hosted uploads,
processed Parquet, and generated reports are temporary: they are lost when an
instance is replaced and are not shared across instances. Download exports you
want to keep. Sessions may reconnect when the hosting function reaches its time
limit. This deployment is intended for the included small demonstration dataset.

GitHub auto-deployment requires connecting the GitHub account in Vercel's account
settings and linking `tusharkshirsagar13/AirScope` under the project's Git settings.
Until then, deploy updates from the linked checkout with `npx vercel --prod`.

## 8. Using your own dataset

1. Copy the file into `data/raw/` (or use **Upload a dataset** in the sidebar, ≤ 200 MB;
   larger files should be copied into `data/raw/`).
2. Pick it in the sidebar, or make it the default via `dataset.path` in `config/settings.yaml`.
3. If columns are not recognised, open **Dataset notes** in the app and add the header
   names to `config/column_mapping.yaml`. Set your missing-value code in
   `dataset.missing_sentinels`.

## 9. Command-line tools

```bash
python scripts/run_pipeline.py            # clean + analyse; writes data/reports/*
python scripts/run_pipeline.py --force    # ignore cached Parquet
python scripts/benchmark.py --scale 50    # Pandas vs Spark timing (rows replicated for timing only)
```

## 10. Tests

```bash
bash scripts/run_tests.sh        # macOS/Linux: compile check + pytest
scripts\run_tests.bat            # Windows
python -m pytest -v              # directly
```
38 tests cover loading, format detection, column mapping, numeric/date parsing,
missing codes, range validation, duplicates, empty/header-only files, datasets without
location/AQI/date columns, JSON/Parquet/Excel input, every analytics function,
observations, exports, Parquet re-use, and a headless start of the dashboard.

## 11. Troubleshooting

| Symptom | Fix |
|---|---|
| "Apache Spark could not start" / `JAVA_GATEWAY_EXITED` | Install JDK 17/21; check `java -version`; set `JAVA_HOME`. |
| `UnsupportedClassVersionError` | Java too old for Spark 4 — use JDK 17+. |
| Windows: Parquet write warning mentioning winutils / HADOOP_HOME | The app continues in memory automatically. To persist Parquet, install `winutils.exe` for Hadoop 3 and set `HADOOP_HOME`, or use WSL. |
| `Python worker failed to connect back` | Run from the activated `.venv`; the app sets `PYSPARK_PYTHON` to the current interpreter. |
| Out of memory on a large file | Raise `spark.driver_memory` in `settings.yaml`, set `cache_clean_data: false`. |
| Port 8501 busy | `streamlit run app.py --server.port 8502` |
| Columns not detected | See *Dataset notes* in the app; add aliases in `column_mapping.yaml`. |
| Stale results after editing data | Click **Re-process dataset** in the sidebar (or delete `data/processed/`). |

## 12. Known limitations

- The supplied dataset is one site, so true *city-wise* comparison is not possible;
  the location view activates automatically for datasets with a location column.
- No AQI is computed — the dataset has no AQI column and lacks the pollutants/averaging
  periods an official AQI requires (e.g. PM2.5, PM10).
- ~9 k rows is small: it demonstrates Spark techniques, not production-scale performance.
  Local-mode Spark is not a cluster; on this data Pandas is faster (see benchmark).
- Median/quartiles use Spark's approximate `percentile_approx`.
- Educational tool only — not a regulatory monitoring system or medical advice.

## 13. Attribution

Dataset: Vito, S. (2008). *Air Quality* [Dataset]. UCI Machine Learning Repository.
https://doi.org/10.24432/C59K5F — licensed CC BY 4.0.
