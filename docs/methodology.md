# AirScope — Methodology

This document explains how data moves through AirScope and why each design decision
was made. File references point to the implementing module.

```
 data/raw/<file>          (never modified)
      │  Extract  ─ src/data_loader.py
      ▼
 Spark DataFrame, all columns as text, safe column names
      │  Map      ─ config/column_mapping.yaml → map_columns()
      │  Clean    ─ src/data_cleaning.py
      ▼
 typed, validated DataFrame  + cleaning_report.json
      │  Load     ─ src/spark_processor.py
      ▼
 data/processed/<stem>_<fingerprint>/clean.parquet/year=YYYY/month=M/…
      │  Analyse  ─ src/analytics.py   (filters → Spark aggregations)
      ▼
 small pandas tables  →  src/insights.py (observations)  →  app.py (Plotly charts, exports)
```

## 1. Data ingestion (`data_loader.py`)

1. **Format detection** by extension: CSV/TXT, XLSX, JSON/JSONL, Parquet.
2. **CSV dialect sniffing** reads only the first ~200 lines (cheap for huge files).
   The delimiter is the candidate (`, ; \t |`) that gives the most fields *consistently*
   across lines. A comma decimal separator is detected when the delimiter is not a comma
   and values such as `2,6` appear. For the UCI file: delimiter `;`, decimal `,`.
3. **Load as text.** Spark reads every column as a string (`inferSchema=false`).
   Conversion happens later so every failed conversion can be counted; automatic
   inference would silently turn bad values into nulls.
4. **Safe names.** Headers such as `PT08.S1(CO)` contain dots/brackets that break Spark
   SQL column references, so they are renamed (`pt08_s1_co`); originals are kept for reports.
5. **Empty unnamed columns** (the UCI file ends every line with `;;`) are dropped only
   if they contain no data at all.
6. Excel is read with pandas then converted to a Spark DataFrame (Spark has no native
   Excel reader) — fine for spreadsheet-sized files; large data should be CSV/Parquet.

### Column mapping
Headers are normalised (lower-case, letters/digits only: `CO(GT)` → `cogt`) and matched
**exactly** against aliases in `config/column_mapping.yaml`. A column is never assigned
two meanings; if several columns match one meaning, the first is used and a warning is
shown. Unmatched columns are listed as *unmapped* and not analysed. Each mapped
measurement carries a label, unit, kind (pollutant / aqi / sensor / meteorological) and
plausibility limits. Notably, `PT08.S1(CO)` (raw sensor response) is mapped as a
*sensor*, not as a CO concentration.

## 2. Data cleaning (`data_cleaning.py`)

All rules run as Spark expressions in a few passes; counts are gathered with a single
aggregation over flag columns.

| Step | Rule | Effect |
|---|---|---|
| Blank rows | every cell empty | removed, counted |
| Numeric parsing | trim, remove spaces, `,`→`.` for comma-decimal files, cast to double; `NaN`/`Inf` rejected | non-numeric text counted as *unparseable* |
| Missing code | value ∈ `missing_sentinels` (UCI: `-200`) | set to null, counted |
| Plausibility | outside `valid_min`/`valid_max` for **that** measurement (e.g. RH 0–100 %, temperature −50…60 °C, CO 0–50 mg/m³) | set to null, counted; never clipped |
| Timestamp | date + time combined; the format that parses the most rows is chosen (ties → config order); fall-back only to unambiguous year-first ISO formats | rows without a timestamp excluded, counted |
| Location | blank → `(unspecified)` | counted |
| Empty records | no valid measurement left | excluded, counted |
| Duplicates | identical rows | collapsed, counted; same timestamp+location with different values is *reported* but kept |

**No imputation.** Missing or invalid values stay null and are simply left out of the
calculations that need them; each measurement's valid count is reported.

Timestamps are local wall-clock strings with no time zone. They are stored as-is with
the Spark session zone set to UTC, which avoids daylight-saving gaps and makes results
identical on every machine.

Result for the UCI file: 9,471 lines → 114 blank removed → 31 rows with no valid
measurement removed → 0 duplicates → **9,326 rows retained**. Missing-code counts per
column range from 366 (sensors, weather, C6H6) to 8,443 (NMHC).

## 3. Spark processing and storage (`spark_processor.py`)

- **Local SparkSession** `local[*]` (all cores), 2 GB driver memory, 8 shuffle
  partitions (suits a laptop; raise for clusters), UI disabled, non-ANSI casting so
  invalid casts become null (and are counted).
- **Parquet, partitioned by `year`/`month`.** Columnar, compressed and typed, so later
  reads are fast and only needed columns are scanned; partitioning lets date filters
  skip whole folders (*partition pruning*).
- **Fingerprint cache.** Output folder name = hash(file name, size, modified time,
  cleaning config, mapping). Unchanged inputs re-use the Parquet; any change rebuilds.
- **Atomic write.** Output goes to a temporary folder and is renamed only when complete.
- **Graceful fallback.** If Parquet cannot be written (common on Windows without Hadoop
  `winutils`), processing continues with an in-memory cached DataFrame and a warning.

## 4. Aggregation and analysis (`analytics.py`)

Filters (date range, locations) are applied lazily; Spark executes only when a result is
requested. Every function returns a small table — the raw dataset is never collected.

| Analysis | Spark operation |
|---|---|
| Descriptive statistics | one `agg`: count, avg, stddev_samp, min, max, `percentile_approx` (Q1, median, Q3) |
| Trends | `date_trunc(hour/day/week/month)` + `groupBy` → mean, min, max, count |
| Profiles | `groupBy(hour(ts))`, `dayofweek`, `month`; seasons combine months with a count-weighted mean (meteorological seasons, hemisphere configurable) |
| Weekday vs weekend | conditional `avg(when(...))` in one pass |
| Location comparison | `groupBy(location)` → mean, max, min, std, count, sorted |
| Correlation | Pearson `corr(a, b)` for all pairs in **one** aggregation; Spark's `corr` uses only rows where both values exist (pairwise-complete); pairs with < 30 joint observations are blanked |
| Outliers | Tukey fence `Q3 + 1.5·IQR` (or a chosen percentile) from `percentile_approx`; count, share, top readings, monthly share |
| Highest-pollution days | daily mean via `date_trunc('day')`; days with < 75 % of a full day's readings (P90 of readings/day) are not ranked |
| Histogram | equal-width bins computed with `floor((x − min)/width)` + `groupBy` |
| Completeness | per month, `count(col)/count(*)` |
| Pollutant comparison | monthly mean ÷ overall mean × 100 (index) — see §5 |

**Outliers ≠ alerts.** A flagged reading is unusual *relative to this dataset*. Official
alerts depend on jurisdiction-specific limits and averaging periods (e.g. 8-hour CO).

**Correlation ≠ causation.** Strong CO–benzene correlation (r = 0.93) is consistent with
a shared source such as traffic; the data alone cannot establish cause.

## 5. Visualisation (`app.py`, `ui_theme.py`)

- Plotly charts with one shared style (warm off-white, charcoal text, muted teal accent).
- Trend charts show the period mean with an optional min–max band; gaps are left as gaps.
- Pollutants with **different units are never drawn on one numeric axis.** The comparison
  chart indexes each pollutant to its own mean (= 100), so only relative movement is shown.
- Every query result is cached with `st.cache_data`, keyed by dataset fingerprint +
  filters + parameters; the SparkSession and processed dataset use `st.cache_resource`.
- Empty states explain why something is missing (no location column, no rows after filtering).

## 6. Automatic observations (`insights.py`)

Sentence templates are filled with computed numbers only: period covered, mean/median/
range, highest and lowest month (months with < 25 % of typical readings are not ranked),
season, hour of day, weekday vs weekend, highest day, outlier share, strongest
correlation, and data-gap warnings (< 50 % valid). The same inputs always give the same text.

## 7. Performance considerations

- Only aggregates and bounded samples (≤ 1,000 rows) reach the driver.
- One-pass multi-aggregations (statistics, correlation, cleaning counts) minimise scans.
- The cleaned DataFrame is cached in Spark memory while the dashboard runs
  (`cache_clean_data`, disable for data larger than RAM).
- Parquet + column pruning + partition pruning reduce I/O for repeated queries.

### Benchmark (`benchmark.py`)
Same job in both engines: read Parquet → monthly mean of all 13 measurements → collect.
One warm-up run excluded; median of 3; results checked for equality.

Measured in the build environment (Linux, 2 CPUs, Spark 4.2.0 `local[*]`, pandas 2.3.3):

| Rows | Pandas | Spark | Results equal |
|---|---|---|---|
| 9,326 | 0.015 s | 0.81 s | yes |
| 466,300 (×50 replicated, timing only) | 0.079 s | 0.79 s | yes |

Pandas is faster here: the data fits easily in memory and Spark pays fixed job-scheduling
costs. Spark's time stayed flat while Pandas' grew ~5×, illustrating how the trade-off
shifts with volume. Replicated rows compress unusually well in Parquet, so this is not a
realistic large-data test, and **local mode is not a cluster**. Spark becomes the better
tool when data exceeds one machine's memory or is spread across many nodes.

## 8. AQI interpretation

AQI scales differ by country (India's NAQI, US EPA AQI, EU CAQI) in pollutants,
breakpoints and averaging periods. The supplied dataset has no AQI column and lacks
PM2.5/PM10, so **no AQI is calculated**. If a future dataset includes an AQI column it is
analysed as supplied, labelled "as supplied (scale not verified)", and no official
category names are attached unless the standard is documented.

## 9. Limitations and reproducibility

- Results are reproducible: same file + same config → same Parquet fingerprint, same
  numbers. `percentile_approx` is deterministic for a given input and accuracy.
- Approximate quartiles; seasonal/monthly profiles pool different years.
- Single site; NMHC available almost only for March–April 2004 (9.8 % of rows).
- The final month (April 2005) holds only ~4 days of data.
- Educational analysis only; not regulatory monitoring or health advice.
