# data/reports — analysis outputs (generated)

Written by `python scripts/run_pipeline.py` and `python scripts/benchmark.py`:

- `descriptive_statistics.csv` — count, mean, std, min, quartiles, max per measurement
- `correlation_matrix.csv` — Pearson r between pollutants (pairwise-complete)
- `monthly_<pollutant>.csv` — monthly mean / min / max / count
- `airscope_report.json` — dataset info, processing statistics, results, observations, limitations
- `benchmark_x<N>.json` — Pandas vs Spark timing (environment included)

The dashboard offers the same summaries as CSV downloads and a JSON report.
