"""Optional, fair Pandas-vs-Spark benchmark.

Both engines run the SAME calculation on the SAME cleaned Parquet input:

    read Parquet -> monthly mean of every measurement column -> collect result

Timings include reading the file, the aggregation and bringing the (tiny)
result back. Each engine is run several times and the median is reported.
Results of the two engines are compared to confirm they are equivalent.

`scale > 1` replicates the cleaned rows N times (written once to a separate
benchmark folder) purely to observe how timings change with volume. Replicated
rows are never used for any analytical result.

Caveat: Spark here runs in local mode on one machine. This is not a cluster
benchmark, and on small inputs Spark is usually SLOWER than Pandas because of
job scheduling overhead. Spark's advantage appears when data no longer fits
comfortably in one machine's memory or when work is spread across nodes.
"""
from __future__ import annotations

import os
import platform
import shutil
import statistics
import time
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .data_cleaning import TS


def _dir_size(p: Path) -> int:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) if p.is_dir() else p.stat().st_size


def prepare_input(spark: SparkSession, parquet_dir: Path, scale: int) -> Path:
    if scale <= 1:
        return parquet_dir
    out = parquet_dir.parent / f"benchmark_x{scale}.parquet"
    if not out.exists():
        df = spark.read.parquet(str(parquet_dir)).drop("year", "month")
        rep = df.crossJoin(spark.range(scale).withColumnRenamed("id", "__copy")).drop("__copy")
        tmp = out.with_name(out.name + ".tmp")
        shutil.rmtree(tmp, ignore_errors=True)
        rep.write.mode("overwrite").parquet(str(tmp))
        tmp.rename(out)
    return out


def run_benchmark(spark: SparkSession, parquet_dir: Path, measurement_cols: list[str],
                  repeats: int = 3, scale: int = 1) -> dict[str, Any]:
    src = prepare_input(spark, Path(parquet_dir), scale)
    cols = list(measurement_cols)

    def pandas_job() -> pd.DataFrame:
        pdf = pd.read_parquet(src, columns=[TS] + cols)
        pdf["period"] = pdf[TS].dt.to_period("M").dt.to_timestamp()
        return pdf.groupby("period")[cols].mean().sort_index()

    def spark_job() -> pd.DataFrame:
        sdf = spark.read.parquet(str(src)).select(TS, *cols)
        res = (sdf.groupBy(F.date_trunc("month", F.col(TS)).alias("period"))
               .agg(*[F.avg(c).alias(c) for c in cols]).orderBy("period").toPandas())
        return res.set_index("period")

    def time_it(fn) -> tuple[list[float], pd.DataFrame]:
        fn()  # warm-up (JVM JIT, file cache) — excluded from timings
        times, out = [], None
        for _ in range(repeats):
            t0 = time.perf_counter()
            out = fn()
            times.append(time.perf_counter() - t0)
        return times, out

    p_times, p_res = time_it(pandas_job)
    s_times, s_res = time_it(spark_job)
    p_res.index = pd.to_datetime(p_res.index)
    s_res.index = pd.to_datetime(s_res.index)
    diff = (p_res[cols] - s_res.reindex(p_res.index)[cols]).abs().max().max()
    rows = spark.read.parquet(str(src)).count()

    return {
        "operation": "read Parquet + monthly mean of each measurement + collect",
        "rows": rows,
        "scale_factor": scale,
        "replicated_rows_note": "rows replicated for timing only" if scale > 1 else "original cleaned rows",
        "input_size_mb": round(_dir_size(src) / 1e6, 2),
        "repeats": repeats,
        "pandas_seconds": [round(t, 4) for t in p_times],
        "spark_seconds": [round(t, 4) for t in s_times],
        "pandas_median_s": round(statistics.median(p_times), 4),
        "spark_median_s": round(statistics.median(s_times), 4),
        "results_equivalent": bool(diff < 1e-6) if pd.notna(diff) else False,
        "max_abs_difference": float(diff) if pd.notna(diff) else None,
        "environment": {
            "machine": platform.machine(), "system": f"{platform.system()} {platform.release()}",
            "cpu_count": os.cpu_count(), "python": platform.python_version(),
            "spark": spark.version, "spark_master": spark.sparkContext.master,
            "pandas": pd.__version__,
        },
        "caveat": ("Local-mode Spark on a single machine; not representative of a distributed cluster. "
                   "Spark is not always faster — for small data its scheduling overhead usually dominates."),
    }
