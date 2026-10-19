# Air Quality and Pollution Data Analytics Using Big Data Processing

### (AirScope — Air Quality Intelligence)

---

## Title page

| Field | Value |
|---|---|
| Project title | Air Quality and Pollution Data Analytics Using Big Data Processing |
| Subject | Big Data Analysis — Mini Project |
| Student name(s) & roll no(s). | **[TO BE FILLED IN]** |
| Department | **[TO BE FILLED IN]** |
| College / Institute | **[TO BE FILLED IN]** |
| Academic year | **[TO BE FILLED IN]** |
| Guide | **[TO BE FILLED IN]** |
| Date of submission | **[TO BE FILLED IN]** |

> Sections marked **[TO BE FILLED IN]** require your details. Apply your institution's
> formatting (certificate page, declaration, acknowledgement, page numbering) when
> converting this file to the final report.

---

## Abstract

Urban air-quality monitoring generates continuous, high-frequency measurements that are
frequently incomplete and inconsistently formatted. This project, **AirScope**, builds an
end-to-end analytics pipeline on **Apache Spark (PySpark)** that ingests such data, applies
transparent validation rules, stores the cleaned result as **year/month-partitioned
Parquet**, and computes temporal trends, daily and seasonal profiles, correlations and
statistical outliers through distributed DataFrame operations. Results are presented in a
**Streamlit** dashboard with **Plotly** charts and automatically generated, data-driven
observations. Applied to the UCI *Air Quality* dataset (9,471 lines; 9,326 hourly records
retained from one road-level site in an Italian city, March 2004 – April 2005), the system
shows a pronounced traffic-like daily cycle (CO peaking at 19:00 at 3.73 mg/m³ versus
0.71 mg/m³ at 05:00), weekday CO levels 42.6 % above weekends, strong correlation between
CO and benzene (r = 0.93), and large gaps in the NMHC record (valid in only 9.8 % of rows).
Every excluded value is counted and reported; no values are imputed and no AQI is invented.

---

## 1. Introduction

Air pollution is among the most significant environmental health risks in cities.
Monitoring stations and low-cost sensors record pollutant concentrations every hour,
producing datasets that grow quickly in **volume**, arrive continuously (**velocity**),
mix reference analysers, sensors and weather data (**variety**), and suffer from sensor
faults and missing values (**veracity**). Turning them into **value** — understandable
patterns — requires scalable processing and careful data handling. This project applies
Big Data tools to that problem at mini-project scale while keeping the design ready for
larger inputs.

## 2. Problem statement

Design and implement a system that ingests raw air-quality data, cleans and validates it
reproducibly using a big-data processing engine, and analyses pollution patterns over time
(and across locations when available), presenting results in an interactive dashboard —
without fabricating data, results or categories that the dataset does not support.

## 3. Motivation

- Spreadsheet analysis hides data-quality problems (e.g. `-200` missing codes averaged as
  real values would drag every mean down).
- Single-machine tools stop scaling as monitoring networks grow.
- Students and analysts need a transparent, explainable pipeline in which every number
  can be traced back to a rule.

## 4. Objectives

1. Ingest multiple file formats with PySpark and detect their structure automatically.
2. Provide a configurable column-mapping layer instead of hard-coded column names.
3. Clean and validate data with explicit, per-pollutant rules, reporting every exclusion.
4. Store processed data efficiently (Parquet, partitioned) and re-use it.
5. Compute trends, profiles, comparisons, correlations and outliers with Spark.
6. Build a simple, professional dashboard with honest labelling and exports.
7. Verify correctness with automated tests and measure Spark vs Pandas fairly.

## 5. Scope

**In scope:** batch analysis of historical tabular data (CSV/Excel/JSON/Parquet); local
Spark execution; descriptive and exploratory statistics; interactive visualisation; CSV/JSON export.

**Out of scope:** real-time streaming ingestion, multi-node cluster deployment, pollutant
forecasting models, official AQI computation and health advice.

## 6. Existing approach and limitations

| Typical approach | Limitation |
|---|---|
| Manual spreadsheet analysis | Not reproducible; missing-value codes easily treated as data; does not scale |
| Single Pandas script | Entire dataset must fit in memory; usually no audit trail of discarded rows |
| Generic BI dashboards | Display raw values; little control over cleaning rules; units often mixed on one axis |
| Official portals | Fixed views; cannot analyse your own dataset |

## 7. Proposed system

AirScope is a layered ETL + analytics system:

1. **Ingestion** (Spark) with format/delimiter/decimal detection.
2. **Mapping** via `config/column_mapping.yaml` (aliases, units, valid ranges).
3. **Cleaning & validation** (Spark) with a full cleaning report.
4. **Storage** as partitioned Parquet with fingerprint-based re-use.
5. **Analytics** (Spark SQL/DataFrame aggregations) returning only small result tables.
6. **Presentation** (Streamlit + Plotly) with observations, filters and exports.

## 8. Hardware and software requirements

| | Minimum / used |
|---|---|
| CPU | Any 64-bit dual-core (developed & tested on 2 vCPUs) |
| RAM | 4 GB (8 GB recommended); Spark driver configured for 2 GB |
| Disk | ~1.5 GB for Python packages + Spark; ~1 MB for this dataset's outputs |
| OS | macOS, Windows 10/11, Linux |
| Python | 3.10 – 3.13 (tested 3.13.16) |
| Java | JDK 17 or 21 (tested OpenJDK 21) |
| Libraries | PySpark 4.2.0, pandas 2.3.3, PyArrow 26.0.0, NumPy 2.5.4, Streamlit 1.66.0, Plotly 7.1.0, PyYAML 6.0.3, openpyxl 3.1.5, pytest 9.1.1 |

## 9. Dataset description

**Source:** UCI Machine Learning Repository, *Air Quality* — Vito, S. (2008),
DOI 10.24432/C59K5F, CC BY 4.0. Hourly averaged responses of a gas multisensor device
and co-located certified reference analysers, deployed at road level in a significantly
polluted area of an Italian city.

| Property | Observed value |
|---|---|
| File | `AirQuality.csv`, 766.7 KB, `;`-delimited, `,` decimal separator |
| Lines | 9,471 = 9,357 data rows + 114 completely blank rows |
| Columns | 15 named + 2 empty trailing columns |
| Time span | 10/03/2004 18:00 → 04/04/2005 14:00, hourly |
| Date / time format | `dd/MM/yyyy` + `HH.mm.ss` |
| Missing-value code | `-200` |
| Locations | One site (no location column) |
| AQI, PM2.5, PM10, SO2 | Not present |

| Column | Meaning | Unit | Valid values* |
|---|---|---|---|
| CO(GT) | Carbon monoxide (reference) | mg/m³ | 7,674 |
| NMHC(GT) | Non-methane hydrocarbons (reference) | µg/m³ | 914 |
| C6H6(GT) | Benzene (reference) | µg/m³ | 8,991 |
| NOx(GT) | Nitrogen oxides (reference) | ppb | 7,718 |
| NO2(GT) | Nitrogen dioxide (reference) | µg/m³ | 7,715 |
| PT08.S1–S5 | Metal-oxide sensor responses | none (raw) | 8,991 each |
| T, RH, AH | Temperature, relative humidity, absolute humidity | °C, %, unstated | 8,991 each |

\*Non-blank rows minus `-200` codes; no other invalid values were found.

The dataset is far smaller than typical "big data"; it is used to demonstrate Spark
techniques that apply unchanged to much larger inputs.

## 10. Data preprocessing

| Stage | Rows |
|---|---|
| Lines read by Spark | 9,471 |
| Blank rows removed | −114 |
| Rows without a parseable timestamp | 0 |
| Rows without any valid measurement (all values `-200`) | −31 |
| Exact duplicates removed | 0 |
| **Retained** | **9,326** |

Value-level: `-200` codes converted to null (CO 1,683; NMHC 8,443; NOx 1,639; NO2 1,642;
other columns 366 each). No non-numeric or out-of-range values were found. Negative
temperatures (minimum −1.9 °C) are valid and kept. No value was imputed. Timestamp format
`dd/MM/yyyy HH.mm.ss` was selected automatically because it parsed all 9,357 rows versus
3,597 for `MM/dd/yyyy`.

## 11. Big data methodology

- **Spark DataFrames & Spark SQL functions** for all heavy work: parsing, validation,
  grouping, percentile estimation, correlation.
- **Lazy evaluation**: filters build a query plan; work happens only when results are requested.
- **Columnar storage**: Parquet with Snappy compression; **partitioning** by year and
  month enables partition pruning.
- **Minimal data movement**: one-pass multi-aggregations; only aggregates and ≤1,000-row
  samples are collected to the driver.
- **Reproducibility**: configuration-hashed outputs; deterministic rules; UTC session time zone.
- **Benchmark**: identical Pandas and Spark jobs, timed fairly (§20).

## 12. System architecture

```
┌──────────────┐   ┌─────────────────────────── Apache Spark (local[*]) ───────────────────────────┐
│ data/raw     │──▶│ Ingest (data_loader) → Map (column_mapping.yaml) → Clean (data_cleaning)      │
│ CSV/XLSX/... │   │          │                                                                     │
└──────────────┘   │          ▼                                                                     │
                   │ Parquet store (spark_processor): data/processed/…/year=/month=                 │
                   │          │                                                                     │
                   │          ▼                                                                     │
                   │ Analytics (analytics.py): groupBy / date_trunc / corr / percentile_approx      │
                   └──────────┼─────────────────────────────────────────────────────────────────────┘
                              ▼ small pandas tables
            insights.py (observations) · export_utils.py (CSV/JSON)
                              ▼
                 Streamlit dashboard (app.py) + Plotly charts
```

## 13. Module descriptions

| Module | Responsibility |
|---|---|
| `src/config.py` | Loads YAML settings and mapping; resolves all paths relative to the project root |
| `src/data_loader.py` | SparkSession creation; format detection; CSV dialect sniffing; text-only loading; safe column names; column mapping |
| `src/data_cleaning.py` | Numeric parsing, missing codes, range checks, timestamp detection, duplicates, cleaning report |
| `src/spark_processor.py` | ETL orchestration, partitioned Parquet output, fingerprint cache, atomic writes, fallback |
| `src/analytics.py` | Filters and all analytical Spark queries |
| `src/insights.py` | Deterministic observation sentences |
| `src/export_utils.py` | CSV bytes, JSON report with limitations |
| `src/benchmark.py` | Pandas-vs-Spark timing |
| `src/ui_theme.py` | CSS design system and Plotly styling |
| `app.py` | Dashboard layout, filters, caching, charts, downloads |
| `scripts/` | Launchers, CLI pipeline, CLI benchmark, test runners |

## 14. Data-flow explanation

1. The user selects or uploads a file; `process_dataset` computes its fingerprint.
2. If a matching Parquet output exists it is loaded; otherwise Spark ingests the file as
   text, the mapping layer assigns meanings, and the cleaning stage produces typed
   columns plus a report, which are written atomically to Parquet + JSON.
3. Sidebar filters produce a lazy filtered DataFrame.
4. Each dashboard section requests an aggregate (cached by fingerprint + filters).
5. Observations are generated from those aggregates; exports serialise the same results.

## 15. Algorithms and analytical techniques

- **Delimiter detection**: maximise (fields in header × share of lines with the same field count).
- **Timestamp format selection**: count successful parses per candidate in one Spark
  aggregation; choose the maximum.
- **Descriptive statistics**: mean, sample standard deviation, min, max; quartiles via
  Spark's `percentile_approx` (Greenwald–Khanna-style approximate quantiles, accuracy 10,000).
- **Time aggregation**: `date_trunc` to hour/day/week/month, then `groupBy`.
- **Seasonal profile**: count-weighted combination of monthly means
  (Dec–Feb winter, Mar–May spring, Jun–Aug summer, Sep–Nov autumn; northern hemisphere).
- **Pearson correlation**: r = cov(X,Y)/(σX·σY) over pairwise-complete observations; all
  pairs in one pass; minimum 30 joint observations.
- **Outlier detection (Tukey)**: flag x > Q3 + 1.5 × (Q3 − Q1); alternative percentile rule.
- **Coverage-aware ranking**: a day is ranked only if it has ≥ 75 % of a full day's readings.
- **Indexing for cross-unit comparison**: monthly mean / overall mean × 100.

## 16. Dashboard design

Warm off-white background, charcoal text, a single muted-teal accent with amber for
highlights; IBM Plex Sans typography; subtle borders; numbered sections:

1. Header — name, subtitle, dataset and reporting period.
2. Key indicators — four cards (records, sites, average and peak of the selected measurement;
   an AQI card appears only when the dataset supplies AQI).
3. "What the data says" — automatic observations.
4. Pollution trends — hourly/daily/weekly/monthly mean with min–max band.
5. Location comparison — ranked bars when a location column exists; for this single-site
   dataset an explanatory empty state plus a day-of-week comparison.
6. Pollutant analysis — indexed monthly comparison and statistics table with units.
7. Additional insights — tabs for daily & seasonal patterns, distribution, correlation,
   high readings, data quality, Spark vs Pandas.
8. Data explorer — searchable bounded record table and aggregate tables with CSV export.
9. Footer — project, stack, dataset and method.

Sidebar: dataset selector and upload, date-range filter, location filter (when available),
measurement selector, trend resolution, JSON report download, processing status.

**[Insert dashboard screenshots here when preparing the final report.]**

## 17. Results and observations

All figures computed by AirScope on the full cleaned dataset (10 Mar 2004 – 4 Apr 2005).

**Descriptive statistics (valid readings only)**

| Pollutant | Unit | n | Mean | Median | Std | Min | Max |
|---|---|---|---|---|---|---|---|
| CO | mg/m³ | 7,674 | 2.15 | 1.80 | 1.45 | 0.1 | 11.9 |
| NMHC | µg/m³ | 914 | 218.8 | 150 | 204.5 | 7 | 1,189 |
| Benzene | µg/m³ | 8,991 | 10.08 | 8.2 | 7.45 | 0.1 | 63.7 |
| NOx | ppb | 7,718 | 246.9 | 180 | 213.0 | 2 | 1,479 |
| NO2 | µg/m³ | 7,715 | 113.1 | 109 | 48.4 | 2 | 340 |

**Observations**

1. **Daily cycle.** Every reference pollutant peaks at 19:00 and is lowest at 04:00–05:00
   (CO 3.73 vs 0.71 mg/m³; NOx 364 vs 81 ppb; NO2 150 vs 59 µg/m³; benzene 17.7 vs 2.9 µg/m³),
   with a second morning peak around 08:00–09:00 for CO — a pattern consistent with
   commuting traffic, although the data cannot prove the source.
2. **Weekly cycle.** Weekday averages exceed weekend averages for all pollutants: CO +42.6 %,
   benzene +54.4 %, NOx +38.5 %, NO2 +18.8 %; Sunday is the lowest day for CO.
3. **Seasonality.** August 2004 is the lowest month for CO (1.28 mg/m³), NOx (75.9 ppb),
   NO2 (70.1 µg/m³) and benzene (6.99 µg/m³). CO is highest in December 2004 (2.75 mg/m³),
   NOx in November 2004 (424 ppb), NO2 in February 2005 (161 µg/m³), benzene in October
   2004 (13.5 µg/m³). By season NOx averages 360 ppb in winter vs 111 ppb in summer.
4. **Step change in NOx.** Monthly NOx roughly triples between August (75.9 ppb) and
   September 2004 (280.6 ppb) and remains high, while CO and benzene rise far less. The
   dataset alone cannot tell whether this reflects air quality or a change in measurement;
   it is flagged rather than explained.
5. **Correlation.** CO–benzene r = 0.93, NMHC–benzene 0.90, CO–NMHC 0.89, CO–NOx 0.80,
   NOx–NO2 0.76; weakest benzene–NO2 0.61. All pollutant pairs are positively associated,
   consistent with common combustion sources; correlation does not establish causation.
   NMHC correlations rest on only ~880–890 paired hours.
6. **Unusual readings (Tukey IQR).** CO: 215 readings (2.8 %) above 5.6 mg/m³, concentrated in
   October–December 2004; NOx 435 (5.6 %) above 668 ppb; benzene 228 (2.5 %) above 28.4 µg/m³;
   NO2 107 (1.4 %) above 238 µg/m³. These are dataset-relative outliers, not legal exceedances.
7. **Highest days.** CO daily mean peaked on 24 Dec 2004 (5.32 mg/m³); NOx and benzene on
   23 Nov 2004 (847 ppb; 23.8 µg/m³); NO2 on 11 Feb 2005 (223 µg/m³).
8. **Data quality.** NMHC is valid in only 9.8 % of retained rows, essentially March–April
   2004; CO, NOx and NO2 are ~82–83 % complete; the final month contains ~4 days.

**Spark vs Pandas** — see §20.

## 18. Testing

38 automated pytest tests (all passing in the build environment, Python 3.13.16,
PySpark 4.2.0, OpenJDK 21):

| File | Coverage |
|---|---|
| `test_data_validation.py` (14) | delimiter/decimal sniffing; unsupported, missing and empty files; header-only file; name normalisation; UCI and generic header mapping; ambiguity warnings; unmapped columns; JSON, Parquet and Excel input; datasets without location/AQI and without a date column |
| `test_data_cleaning.py` (11) | comma-decimal parsing; cleaning report counts on a synthetic UCI-style file (blank rows, `-200`, text, out-of-range, bad timestamp, duplicates); negative temperature kept; invalid values nulled not clipped; `dd/MM` vs `MM/dd` and ISO timestamps; location labelling; duplicate-key reporting; raw file unchanged; Parquet re-use and forced rebuild |
| `test_analytics.py` (13) | filters; exact statistics; daily series; hour/weekday/season profiles; location ranking; correlation (+1, −1, insufficient n); IQR & percentile outliers; histogram totals; coverage-aware day ranking; completeness; indexing; bounded samples; deterministic observations; CSV/JSON export; headless dashboard start-up on the real dataset |

Additional checks performed: `compileall` syntax check, the dashboard launched with
`streamlit run` and inspected in a headless Chromium browser at desktop and phone widths
(no exceptions; all charts rendered); every insight tab and the benchmark exercised in the browser.

**[Add your own screenshots / test run output if your institution requires them.]**

## 19. Limitations

- Single monitoring site: city-wise comparison is implemented but cannot be demonstrated
  with this dataset.
- No AQI: the dataset lacks AQI and particulate pollutants; computing one would be invalid.
- Small dataset (~9 k rows); local-mode Spark is not a distributed cluster.
- Approximate quartiles; seasonal profiles pool months from different years.
- Missing-data heavy pollutants (NMHC) give less representative results.
- Educational tool; not regulatory monitoring or medical advice.

## 20. Performance evaluation (benchmark)

Job: read cleaned Parquet → monthly mean of 13 measurements → collect. Warm-up excluded,
median of 3 runs, results verified equal. Environment: Linux x86-64, 2 CPUs, Spark 4.2.0
`local[*]`, pandas 2.3.3.

| Rows | Pandas | Spark |
|---|---|---|
| 9,326 | 0.015 s | 0.81 s |
| 466,300 (rows replicated ×50 for timing only) | 0.079 s | 0.79 s |

Pandas is faster on this machine and data size because Spark has fixed job-scheduling
overhead. Spark's time stayed roughly constant while Pandas' grew about 5×, showing how
the balance shifts with volume. Spark's real advantages — data larger than memory and
work spread over many nodes — are not exercised here. *Re-run on your laptop with
`python scripts/benchmark.py --scale 50` and report your own numbers.*

## 21. Future enhancements

- Run on a multi-node Spark cluster or Databricks with multi-year, multi-city data
  (e.g. CPCB station data for Indian cities) to exercise location comparison.
- Spark Structured Streaming for live sensor feeds.
- Jurisdiction-specific AQI (e.g. India NAQI) when PM2.5/PM10 and proper averaging are available.
- Forecasting with Spark MLlib; sensor calibration models (the original purpose of this dataset).
- Weather-adjusted analysis using temperature and humidity.
- Map views for geo-referenced stations.

## 22. Conclusion

AirScope shows how a big-data engine can be applied responsibly to environmental data:
Spark handles ingestion, validation, storage and aggregation; Parquet provides efficient
re-usable storage; and the dashboard communicates results without overstating them. On
the UCI dataset it reveals clear daily, weekly and seasonal pollution cycles and strong
inter-pollutant correlation, while transparently reporting heavy data gaps and a NOx step
change that warrants caution. The same pipeline accepts larger and multi-location
datasets through configuration alone.

## 23. References and dataset attribution

1. Vito, S. (2008). *Air Quality* [Dataset]. UCI Machine Learning Repository.
   https://doi.org/10.24432/C59K5F (CC BY 4.0).
2. De Vito, S., Massera, E., Piga, M., Martinotto, L., & Di Francia, G. (2008). On field
   calibration of an electronic nose for benzene estimation in an urban pollution
   monitoring scenario. *Sensors and Actuators B: Chemical*, 129(2), 750–757.
3. Apache Spark documentation — https://spark.apache.org/docs/latest/
4. Apache Parquet documentation — https://parquet.apache.org/docs/
5. Streamlit documentation — https://docs.streamlit.io/
6. Plotly Python documentation — https://plotly.com/python/
7. Tukey, J. W. (1977). *Exploratory Data Analysis*. Addison-Wesley.

**[Add any references required by your department's format.]**
