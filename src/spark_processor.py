"""End-to-end Spark processing pipeline (ETL):

    Extract  – load the raw file with Spark (data_loader)
    Transform – map columns, clean and validate (data_cleaning)
    Load     – write the cleaned data as Parquet, partitioned by year/month,
               plus a JSON cleaning report

The cleaned Parquet output is re-used on later runs as long as the source file
and the configuration are unchanged (fingerprint check), so the expensive
cleaning job runs once. The raw file is never modified.
"""
from __future__ import annotations

import json
import logging
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import config_fingerprint, resolve_path
from .data_cleaning import TS, clean
from .data_loader import DatasetError, load_raw, map_columns

log = logging.getLogger("airscope.processor")

REPORT_NAME = "cleaning_report.json"
PARQUET_NAME = "clean.parquet"


@dataclass
class ProcessedDataset:
    df: DataFrame
    report: dict[str, Any]
    output_dir: Path | None
    from_cache: bool
    persisted: bool
    warnings: list[str] = field(default_factory=list)

    @property
    def measurements(self) -> dict[str, dict[str, Any]]:
        return self.report.get("measurements", {})

    @property
    def has_time(self) -> bool:
        return bool(self.report.get("has_timestamp"))

    @property
    def has_location(self) -> bool:
        return bool(self.report.get("has_location"))


def source_fingerprint(path: Path, settings: dict[str, Any], mapping_cfg: dict[str, Any]) -> str:
    st = path.stat()
    relevant = {k: settings.get(k) for k in ("dataset", "processing")}
    relevant["dataset"] = {k: v for k, v in (relevant["dataset"] or {}).items() if k != "path"}
    return config_fingerprint(path.name, st.st_size, st.st_mtime_ns, relevant, mapping_cfg, "v1")


def _output_dir(path: Path, settings: dict[str, Any], fp: str) -> Path:
    base = resolve_path(settings.get("paths", {}).get("processed_dir", "data/processed"))
    safe_stem = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in path.stem)[:60]
    return base / f"{safe_stem}_{fp}"


def process_dataset(spark: SparkSession, path: str | Path, settings: dict[str, Any],
                    mapping_cfg: dict[str, Any], force: bool = False) -> ProcessedDataset:
    """Run (or re-use) the full ingestion + cleaning pipeline for one file."""
    path = Path(path)
    if not path.exists():
        raise DatasetError(f"Dataset not found: {path}. Put the file in data/raw/ or set "
                           "dataset.path in config/settings.yaml.")
    fp = source_fingerprint(path, settings, mapping_cfg)
    out_dir = _output_dir(path, settings, fp)
    parquet_dir = out_dir / PARQUET_NAME
    report_file = out_dir / REPORT_NAME
    reuse = settings.get("processing", {}).get("reuse_processed_parquet", True)

    if reuse and not force and parquet_dir.exists() and report_file.exists():
        try:
            report = json.loads(report_file.read_text(encoding="utf-8"))
            df = spark.read.parquet(str(parquet_dir))
            log.info("Re-using processed Parquet at %s", parquet_dir)
            return ProcessedDataset(df=df, report=report, output_dir=out_dir,
                                    from_cache=True, persisted=True)
        except Exception as exc:  # corrupted cache -> rebuild
            log.warning("Cached output unreadable (%s); rebuilding.", exc)

    t0 = time.perf_counter()
    raw = load_raw(spark, path, settings)
    mapping = map_columns(list(raw.original_names.values()), mapping_cfg)
    result = clean(raw, mapping, settings)
    report = result.report
    report["fingerprint"] = fp
    report["processing_seconds"] = round(time.perf_counter() - t0, 2)
    report["spark_version"] = spark.version

    df = result.df
    warnings: list[str] = []
    persisted = False
    if report["rows"]["retained"] > 0:
        # Partition columns enable partition pruning for date-range queries.
        to_write = df
        if report["has_timestamp"]:
            to_write = (df.withColumn("year", F.year(TS)).withColumn("month", F.month(TS))
                        .repartition("year", "month"))
        tmp_dir = out_dir.parent / f".tmp_{uuid.uuid4().hex[:8]}"
        try:
            out_dir.parent.mkdir(parents=True, exist_ok=True)
            writer = to_write.write.mode("overwrite")
            if report["has_timestamp"]:
                writer = writer.partitionBy("year", "month")
            writer.parquet(str(tmp_dir / PARQUET_NAME))
            (tmp_dir / REPORT_NAME).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
            if out_dir.exists():
                shutil.rmtree(out_dir)
            tmp_dir.rename(out_dir)          # atomic swap: no half-written outputs
            df = spark.read.parquet(str(parquet_dir))
            persisted = True
        except Exception as exc:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = ("Could not write the cleaned Parquet output (on Windows this usually means "
                   f"Hadoop winutils is missing). Continuing in memory. Details: {exc}")
            log.warning(msg)
            warnings.append(msg)
            df = df.cache()
    report["persisted_parquet"] = persisted
    return ProcessedDataset(df=df, report=report, output_dir=out_dir if persisted else None,
                            from_cache=False, persisted=persisted, warnings=warnings)


def list_datasets(settings: dict[str, Any]) -> list[Path]:
    """Data files available in data/raw (and data/raw/uploads)."""
    raw_dir = resolve_path(settings.get("paths", {}).get("raw_dir", "data/raw"))
    exts = {".csv", ".txt", ".xlsx", ".json", ".jsonl", ".parquet"}
    if not raw_dir.exists():
        return []
    files = [p for p in raw_dir.rglob("*") if p.is_file() and p.suffix.lower() in exts
             and not p.name.startswith(".")]
    # A Parquet *directory* is also a valid dataset.
    files += [p for p in raw_dir.rglob("*.parquet") if p.is_dir()]
    return sorted(set(files))
