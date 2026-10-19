"""Data ingestion: Spark session, file-format detection, raw loading and the
column-mapping layer.

Design notes
------------
* Every source column is loaded as a *string* first. Type conversion happens in
  ``data_cleaning`` where every failed conversion can be counted and reported,
  instead of Spark silently turning bad values into nulls during inference.
* Source column names such as ``PT08.S1(CO)`` contain characters that are
  awkward in Spark SQL, so columns are renamed to safe internal names and the
  original names are kept for reporting.
* Column meanings are assigned only through exact (normalised) alias matches
  from ``config/column_mapping.yaml``. Nothing is guessed.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, StructType

log = logging.getLogger("airscope.loader")

SUPPORTED_FORMATS = {".csv": "csv", ".txt": "csv", ".xlsx": "excel", ".json": "json",
                     ".jsonl": "json", ".parquet": "parquet"}


class DatasetError(Exception):
    """A user-facing problem with the dataset (missing, unreadable, empty...)."""


class SparkStartupError(Exception):
    """Spark (or Java) could not be started."""


# --------------------------------------------------------------------------- #
# Spark session
# --------------------------------------------------------------------------- #
def get_spark(settings: dict[str, Any] | None = None) -> SparkSession:
    """Create (or reuse) a local SparkSession tuned for a laptop."""
    cfg = (settings or {}).get("spark", {})
    # Make Spark workers use the same interpreter as the driver (important in venvs
    # and on Windows).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    try:
        builder = (
            SparkSession.builder.appName(cfg.get("app_name", "AirScope"))
            .master(cfg.get("master", "local[*]"))
            .config("spark.driver.memory", cfg.get("driver_memory", "2g"))
            .config("spark.sql.shuffle.partitions", str(cfg.get("shuffle_partitions", 8)))
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            # Non-ANSI mode: failed casts become NULL (we count them explicitly).
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            # Timestamps in the source are local wall-clock strings with no zone.
            # Interpreting them in UTC avoids daylight-saving gaps and keeps results
            # identical on every machine.
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
        )
        spark = builder.getOrCreate()
    except Exception as exc:  # Java missing, port problems, etc.
        raise SparkStartupError(
            "Apache Spark could not start. Spark needs a Java runtime (JDK 17 or 21 "
            "recommended). Check `java -version` in a terminal and see the README "
            f"troubleshooting section. Original error: {exc}"
        ) from exc
    spark.sparkContext.setLogLevel(cfg.get("log_level", "WARN"))
    return spark


# --------------------------------------------------------------------------- #
# Format detection
# --------------------------------------------------------------------------- #
def detect_format(path: Path) -> str:
    fmt = SUPPORTED_FORMATS.get(path.suffix.lower())
    if fmt is None:
        raise DatasetError(
            f"Unsupported file type '{path.suffix}'. Supported: "
            + ", ".join(sorted(SUPPORTED_FORMATS))
        )
    return fmt


@dataclass
class CsvDialect:
    delimiter: str
    decimal: str


def sniff_csv(path: Path, encoding: str = "utf-8", delimiter: str = "auto",
              decimal: str = "auto", sample_lines: int = 200) -> CsvDialect:
    """Detect the delimiter and decimal separator from the first lines of a file.

    Only a small text sample is read, so this is cheap for very large files.
    """
    lines: list[str] = []
    with path.open("r", encoding=encoding, errors="replace") as fh:
        for _ in range(sample_lines):
            line = fh.readline()
            if not line:
                break
            if line.strip().strip(";,|\t"):
                lines.append(line.rstrip("\r\n"))
    if not lines:
        raise DatasetError(f"'{path.name}' is empty — it contains no header or data rows.")

    if delimiter in (None, "", "auto"):
        best, best_score = ",", -1.0
        for cand in [",", ";", "\t", "|"]:
            counts = [len(next(csv.reader(io.StringIO(ln), delimiter=cand), [])) for ln in lines[:50]]
            header_fields = counts[0]
            if header_fields <= 1:
                continue
            consistency = sum(1 for c in counts if c == header_fields) / len(counts)
            score = header_fields * consistency
            if score > best_score:
                best, best_score = cand, score
        delimiter = best
    elif delimiter == "\\t":
        delimiter = "\t"

    if decimal in (None, "", "auto"):
        decimal = "."
        if delimiter != ",":
            body = "\n".join(lines[1:])
            pattern = re.compile(r"(?:^|" + re.escape(delimiter) + r")\s*-?\d+,\d+\s*(?=" + re.escape(delimiter) + r"|$)", re.M)
            if pattern.search(body):
                decimal = ","
    return CsvDialect(delimiter=delimiter, decimal=decimal)


# --------------------------------------------------------------------------- #
# Raw loading
# --------------------------------------------------------------------------- #
def safe_column_names(names: list[str]) -> list[str]:
    """Convert arbitrary headers to unique, Spark-friendly identifiers."""
    out: list[str] = []
    seen: dict[str, int] = {}
    for i, name in enumerate(names):
        base = re.sub(r"[^0-9a-zA-Z]+", "_", (name or "").strip()).strip("_").lower() or f"col_{i}"
        if base[0].isdigit():
            base = f"c_{base}"
        n = seen.get(base, 0)
        seen[base] = n + 1
        out.append(base if n == 0 else f"{base}_{n}")
    return out


@dataclass
class RawDataset:
    df: DataFrame                       # all columns as strings, safe names
    source_path: Path
    file_format: str
    original_names: dict[str, str]      # safe name -> original header
    dialect: CsvDialect | None = None
    dropped_empty_columns: list[str] = field(default_factory=list)

    @property
    def safe_by_original(self) -> dict[str, str]:
        return {v: k for k, v in self.original_names.items()}


def _all_to_string(df: DataFrame) -> DataFrame:
    cols = []
    for f in df.schema.fields:
        if isinstance(f.dataType, (StructType, ArrayType, MapType)):
            cols.append(F.to_json(F.col(f"`{f.name}`")).alias(f.name))
        elif f.dataType.simpleString() == "timestamp":
            cols.append(F.date_format(F.col(f"`{f.name}`"), "yyyy-MM-dd HH:mm:ss").alias(f.name))
        else:
            cols.append(F.col(f"`{f.name}`").cast("string").alias(f.name))
    return df.select(*cols)


def load_raw(spark: SparkSession, path: str | Path, settings: dict[str, Any]) -> RawDataset:
    """Load any supported file into a Spark DataFrame of string columns."""
    path = Path(path)
    if not path.exists():
        raise DatasetError(f"Dataset not found: {path}. Place your file in data/raw/ "
                           "or update dataset.path in config/settings.yaml.")
    if path.is_file() and path.stat().st_size == 0:
        raise DatasetError(f"'{path.name}' is empty (0 bytes).")
    ds_cfg = settings.get("dataset", {})
    fmt = detect_format(path)
    dialect = None

    try:
        if fmt == "csv":
            dialect = sniff_csv(path, ds_cfg.get("encoding", "utf-8"),
                                ds_cfg.get("delimiter", "auto"), ds_cfg.get("decimal", "auto"))
            df = (spark.read.option("header", True).option("sep", dialect.delimiter)
                  .option("inferSchema", False).option("encoding", ds_cfg.get("encoding", "utf-8"))
                  .option("mode", "PERMISSIVE").option("ignoreLeadingWhiteSpace", True)
                  .option("ignoreTrailingWhiteSpace", True).csv(str(path)))
        elif fmt == "json":
            with path.open("r", encoding=ds_cfg.get("encoding", "utf-8"), errors="replace") as fh:
                head = fh.read(4096).lstrip()
            multiline = head.startswith("[")
            df = spark.read.option("multiLine", multiline).json(str(path))
            if "_corrupt_record" in df.columns and len(df.columns) == 1:
                raise DatasetError(f"'{path.name}' is not valid JSON / JSON-lines.")
            df = _all_to_string(df)
        elif fmt == "parquet":
            df = _all_to_string(spark.read.parquet(str(path)))
        else:  # excel — Spark has no native Excel reader; suitable for small files only.
            import pandas as pd
            pdf = pd.read_excel(path, dtype=str)
            if pdf.shape[1] == 0:
                raise DatasetError(f"'{path.name}' has no columns.")
            pdf = pdf.where(pdf.notna(), None)
            from pyspark.sql.types import StringType, StructField
            schema = StructType([StructField(str(c), StringType(), True) for c in pdf.columns])
            df = spark.createDataFrame(pdf.astype(object).values.tolist(), schema=schema)
    except DatasetError:
        raise
    except Exception as exc:
        raise DatasetError(f"Could not read '{path.name}' as {fmt}: {exc}") from exc

    if not df.columns:
        raise DatasetError(f"'{path.name}' contains no columns.")

    originals = list(df.columns)
    safe = safe_column_names(originals)
    df = df.toDF(*safe)
    original_names = dict(zip(safe, originals))

    # Drop unnamed columns that hold no data at all (e.g. trailing ";;" in the UCI CSV).
    unnamed = [s for s, o in original_names.items()
               if (o or "").strip() == "" or re.fullmatch(r"_c\d+", o or "")]
    dropped: list[str] = []
    if unnamed:
        counts = df.agg(*[F.count(F.when(F.trim(F.col(c)) != "", 1)).alias(c) for c in unnamed]).first()
        dropped = [c for c in unnamed if counts[c] == 0]
        if dropped:
            df = df.drop(*dropped)
            for c in dropped:
                original_names.pop(c, None)
    return RawDataset(df=df, source_path=path, file_format=fmt, original_names=original_names,
                      dialect=dialect, dropped_empty_columns=dropped)


# --------------------------------------------------------------------------- #
# Column mapping
# --------------------------------------------------------------------------- #
def normalise_name(name: str) -> str:
    return re.sub(r"[^0-9a-z]", "", (name or "").lower())


@dataclass
class ColumnMapping:
    roles: dict[str, str]                     # role -> original column name
    measurements: dict[str, str]              # canonical key -> original column name
    unmapped: list[str]
    warnings: list[str]
    meta: dict[str, dict[str, Any]]           # canonical key -> label/unit/kind/limits

    def to_dict(self) -> dict[str, Any]:
        return {"roles": self.roles, "measurements": self.measurements,
                "unmapped": self.unmapped, "warnings": self.warnings}


def map_columns(columns: list[str], mapping_cfg: dict[str, Any]) -> ColumnMapping:
    """Assign meanings to source columns using exact normalised alias matches."""
    norm = {c: normalise_name(c) for c in columns}
    used: dict[str, str] = {}   # original column -> what it was assigned to
    warnings: list[str] = []

    def find(aliases: list[str], target: str) -> str | None:
        alias_set = {normalise_name(a) for a in aliases}
        hits = [c for c in columns if norm[c] in alias_set]
        free = [c for c in hits if c not in used]
        if len(hits) > 1:
            warnings.append(f"Several columns match '{target}' ({', '.join(hits)}); "
                            f"using '{(free or hits)[0]}'. Edit config/column_mapping.yaml to change this.")
        if not free:
            if hits:
                warnings.append(f"Column '{hits[0]}' already assigned to '{used[hits[0]]}', "
                                f"so it was not also used as '{target}'.")
            return None
        used[free[0]] = target
        return free[0]

    roles: dict[str, str] = {}
    role_cfg = mapping_cfg.get("roles", {})
    for role in ["datetime", "date", "time", "location"]:
        col = find(role_cfg.get(role, {}).get("aliases", []), role)
        if col:
            roles[role] = col
    if "datetime" in roles and "date" in roles:
        warnings.append(f"Both a datetime column ('{roles['datetime']}') and a date column "
                        f"('{roles['date']}') were found; the datetime column is used.")
    if "time" in roles and "date" not in roles:
        warnings.append(f"A time column ('{roles['time']}') exists without a date column; it is ignored.")

    measurements: dict[str, str] = {}
    meta: dict[str, dict[str, Any]] = {}
    for key, spec in mapping_cfg.get("measurements", {}).items():
        col = find(spec.get("aliases", []), key)
        if col:
            measurements[key] = col
            meta[key] = {
                "label": spec.get("label", key), "name": spec.get("name", key),
                "unit": spec.get("unit", "as reported"), "kind": spec.get("kind", "pollutant"),
                "valid_min": spec.get("valid_min"), "valid_max": spec.get("valid_max"),
                "source_column": col,
            }
    unmapped = [c for c in columns if c not in used]
    if not measurements:
        warnings.append("No measurement columns were recognised. Add your column names to "
                        "config/column_mapping.yaml.")
    if not ("datetime" in roles or "date" in roles):
        warnings.append("No date/time column was recognised — time-based analysis is disabled.")
    if "location" not in roles:
        warnings.append("No city/location/station column was found — the dataset is treated "
                        "as a single monitoring site, so location comparison is not possible.")
    if "aqi" not in measurements:
        warnings.append("No AQI column was found. AirScope analyses pollutant concentrations "
                        "directly and does not invent an AQI.")
    return ColumnMapping(roles=roles, measurements=measurements, unmapped=unmapped,
                         warnings=warnings, meta=meta)
