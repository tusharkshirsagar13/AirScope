# data/processed — cleaned data (generated)

Created automatically by the Spark pipeline. Safe to delete at any time — it is rebuilt
on the next run.

For each source file the pipeline writes a folder `<file-stem>_<fingerprint>/` with:

- `clean.parquet/` — cleaned, typed records, partitioned by `year=`/`month=` (enables
  partition pruning for date filters)
- `cleaning_report.json` — rows loaded / removed / retained and per-column validation counts

The fingerprint combines the file's name, size and modification time with the cleaning
configuration, so editing the data or the config automatically triggers re-processing.
Outputs are written to a temporary folder and renamed when complete, so an interrupted
run never leaves a half-written result.
