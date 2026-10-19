# Viva Questions and Answers — AirScope

Concise answers you can explain in your own words. Numbers refer to the supplied UCI dataset.

---

### Big Data concepts

**1. What is Big Data?**
Data whose volume, speed of arrival or variety is too large or complex for traditional
single-machine tools to store and process efficiently. Big Data systems split storage and
computation across many machines.

**2. Explain the 5 Vs of Big Data with reference to this project.**
- **Volume** – air-quality networks record every pollutant every hour at many stations.
- **Velocity** – new readings arrive continuously (here: hourly).
- **Variety** – reference analysers, raw sensor responses and weather variables, in CSV, Excel, JSON, Parquet.
- **Veracity** – quality problems: 8,443 of 9,357 NMHC values are the missing code `-200`.
- **Value** – patterns such as the 19:00 pollution peak and weekday/weekend difference.

**3. Is this dataset really "big data"?**
No — ~9 k rows (767 KB). The project demonstrates Spark techniques (distributed DataFrames,
Parquet, partitioning, lazy evaluation) that work unchanged on much larger data. We say this
openly in the report and benchmark.

**4. What is ETL? Where is it in your project?**
Extract–Transform–Load. Extract: Spark reads the raw file (`data_loader.py`). Transform:
mapping, validation and cleaning (`data_cleaning.py`). Load: cleaned data written as Parquet
(`spark_processor.py`).

### Apache Spark & PySpark

**5. What is Apache Spark?**
An open-source distributed processing engine. It splits data into partitions and processes
them in parallel on executor cores/machines, keeping intermediate data in memory where possible.

**6. What is PySpark?**
Spark's Python API. Python code builds a query plan; the actual work runs in the Spark JVM,
which is why Java is required.

**7. What is a Spark DataFrame?**
A distributed table with named, typed columns — like a SQL table split across partitions.
Operations are optimised by Spark's Catalyst optimizer.

**8. What are transformations and actions? What is lazy evaluation?**
Transformations (`filter`, `select`, `groupBy`) only build a plan. Actions (`count`,
`collect`, `toPandas`, `write`) execute it. Lazy evaluation lets Spark optimise the whole
plan — e.g. applying our date filter before reading unnecessary partitions.

**9. Which Spark operations does AirScope use?**
`read.csv/json/parquet`, `regexp_replace`, `cast`, `to_timestamp`, `filter`,
`dropDuplicates`, `groupBy` + `agg` (`avg`, `stddev_samp`, `min`, `max`, `count`),
`percentile_approx`, `corr`, `date_trunc`, `hour`, `dayofweek`, `month`, `write.partitionBy`.

**10. What does `local[*]` mean?**
Spark runs on one machine using all CPU cores as parallel workers. Same code can run on a
cluster by changing the master URL.

**11. Spark vs Pandas — which is faster here and why?**
Pandas: 0.015 s vs Spark 0.81 s on 9,326 rows in our benchmark. Pandas works in one
process's memory with no scheduling overhead; Spark must plan and schedule jobs. Spark wins
when data exceeds one machine's memory or can be spread across many nodes. Spark is not
always faster.

**12. Hadoop vs Spark?**
Hadoop MapReduce writes intermediate results to disk between steps; Spark keeps them in
memory, so iterative/interactive work is much faster. Hadoop also provides HDFS storage and
YARN resource management, which Spark can use. Spark is the compute engine; it often runs on
Hadoop infrastructure.

**13. What is partitioning and why partition Parquet by year/month?**
Partitioning splits data into folders by column value (`year=2004/month=11`). A query for a
date range can skip irrelevant folders entirely (*partition pruning*), reducing I/O.

### Data cleaning

**14. What cleaning steps did you apply?**
Remove blank rows (114); convert comma decimals; treat `-200` as missing; reject non-numeric
text and values outside per-pollutant plausible ranges; parse timestamps; drop rows with no
valid measurement (31); remove exact duplicates (0). Retained 9,326 rows. Each step is counted.

**15. How did you handle missing values? Why not fill them?**
They stay null and are excluded only from calculations that need that column. Filling
(mean/zero/interpolation) would invent measurements and bias results — e.g. NMHC is 90 %
missing; imputing it would fabricate most of the series.

**16. How do you detect the date format automatically?**
Try each candidate format (e.g. `dd/MM/yyyy HH.mm.ss`, `MM/dd/yyyy …`) in one Spark
aggregation and count successful parses; the best wins. Here `dd/MM` parsed 9,357 rows,
`MM/dd` only 3,597, so 10/03/2004 is correctly 10 March.

**17. Why load everything as strings first?**
So every failed conversion can be counted and reported instead of silently becoming null
through schema inference.

### Analysis & statistics

**18. What is data aggregation? Give an example.**
Summarising many rows into fewer: `groupBy(date_trunc('month', ts)).agg(avg('co'))` gives
monthly CO means (Dec 2004 highest, 2.75 mg/m³).

**19. What is correlation? What did you find?**
Pearson r measures linear association from −1 to +1. CO–benzene r = 0.93 (strong positive).
Correlation does not imply causation — both may share a cause such as traffic.

**20. How do you detect outliers?**
Tukey's IQR rule: flag values > Q3 + 1.5 × IQR. For CO: Q1 = 1.1, Q3 = 2.9 → threshold
5.6 mg/m³ → 215 readings (2.8 %). These are statistical outliers, not official alerts.

**21. Why is median sometimes better than mean here?**
Pollution data is right-skewed (a few very high hours). CO mean 2.15 > median 1.80; the
median is more robust to extremes.

**22. How do you compare pollutants with different units?**
Never on one numeric axis. Each pollutant's monthly mean is divided by its own overall mean
(× 100), showing relative change only.

### Air-quality domain

**23. What is AQI? Why doesn't AirScope show one?**
An index that converts pollutant concentrations into a single scale with health categories,
using country-specific breakpoints and averaging periods (India NAQI, US EPA AQI, EU CAQI).
This dataset has no AQI column and no PM2.5/PM10, so computing one would be invalid.

**24. What are PM2.5 and PM10?**
Particulate matter with diameters ≤ 2.5 µm and ≤ 10 µm. PM2.5 penetrates deep into the
lungs. Neither is present in this dataset; AirScope supports them if present.

**25. What are CO, NOx, NO2 and benzene, and their units here?**
CO – carbon monoxide (mg/m³); NOx – nitrogen oxides, NO + NO2 (ppb); NO2 – nitrogen dioxide
(µg/m³); C6H6 – benzene (µg/m³). All largely from combustion, especially traffic.

**26. What are the PT08 columns?**
Raw responses of metal-oxide gas sensors — no physical unit and uncalibrated. They are
analysed separately from reference concentrations.

### Storage, tools, testing

**27. CSV vs Parquet?**
CSV: row-based text, no types, large, must be parsed fully. Parquet: columnar, compressed,
typed, supports reading only needed columns and partition pruning — much faster for analytics.

**28. Why Streamlit?**
Builds an interactive web dashboard in pure Python with widgets, caching
(`st.cache_data`, `st.cache_resource`) and downloads — no separate frontend needed.

**29. Why Plotly?**
Interactive charts (hover, zoom, legend toggles) that integrate with Streamlit and resize automatically.

**30. How did you test the project?**
38 pytest tests with synthetic data: loading, mapping, parsing, missing codes, ranges,
duplicates, empty files, missing columns, all analytics (checked against hand-computed
values), exports, Parquet re-use and a headless dashboard start-up. All pass.

**31. How do you avoid collecting big data to the driver?**
Only aggregates and bounded samples (≤ 1,000 rows) are converted to Pandas; the full table
stays in Spark.

### Limitations & future scope

**32. Main limitations?**
Single site (no city comparison), no AQI/PM data, small dataset, local-mode Spark, approximate
quartiles, NMHC 90 % missing, unexplained NOx step change after Aug 2004.

**33. Future scope?**
Multi-city data on a Spark cluster, Structured Streaming for live feeds, India NAQI when
PM data exists, MLlib forecasting and sensor calibration, map visualisation.
