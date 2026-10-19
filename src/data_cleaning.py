"""Data cleaning and validation with PySpark.

Every rule is transparent: each value that is not used is counted under a
specific reason, and nothing is ever imputed. The cleaning report produced
here is shown in the dashboard and saved as JSON next to the Parquet output.

Per-measurement outcomes (mutually exclusive):
    empty         – cell was blank
    unparseable   – text that is not a number (e.g. "n/a")
    sentinel      – publisher's "missing" code (e.g. -200 in the UCI file)
    below_min     – below the plausibility limit in column_mapping.yaml
    above_max     – above the plausibility limit
    valid         – kept

Row-level outcomes (applied in this order):
    blank rows removed                    – every cell empty
    rows without a parseable timestamp    – excluded (only when a date column exists)
    rows without any valid measurement    – excluded
    exact duplicate rows                  – collapsed to one
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import reduce
from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .data_loader import ColumnMapping, RawDataset

log = logging.getLogger("airscope.cleaning")

TS = "ts"
LOC = "location"


@dataclass
class CleanResult:
    df: DataFrame
    report: dict[str, Any]


def _any(conds: list[Column]) -> Column:
    return reduce(lambda a, b: a | b, conds) if conds else F.lit(False)


def parse_numeric(col: Column, decimal: str = ".") -> Column:
    """Text -> double. Handles comma decimals and surrounding spaces.

    NaN / Infinity strings are treated as non-numeric (returned as NULL).
    """
    txt = F.regexp_replace(F.trim(col), r"\s+", "")
    if decimal == ",":
        txt = F.regexp_replace(F.regexp_replace(txt, r"\.", ""), ",", ".")
    val = txt.cast("double")
    return F.when(F.isnan(val) | (F.abs(val) == float("inf")), F.lit(None)).otherwise(val)


def _timestamp_candidates(mapping: ColumnMapping, o2s: dict[str, str],
                          proc: dict[str, Any]) -> tuple[Column | None, list[str]]:
    """Return the raw timestamp text expression and the candidate formats."""
    date_fmts = proc.get("date_formats", ["yyyy-MM-dd"])
    time_fmts = proc.get("time_formats", ["HH:mm:ss"])
    dt_fmts = proc.get("datetime_formats", ["yyyy-MM-dd HH:mm:ss"])
    roles = mapping.roles
    if "datetime" in roles:
        return F.trim(F.col(o2s[roles["datetime"]])), dt_fmts + date_fmts
    if "date" in roles:
        d = F.trim(F.col(o2s[roles["date"]]))
        if "time" in roles:
            t = F.trim(F.col(o2s[roles["time"]]))
            txt = F.when(t.isNull() | (t == ""), d).otherwise(F.concat_ws(" ", d, t))
            combined = [f"{a} {b}" for a in date_fmts for b in time_fmts]
            return txt, combined + date_fmts
        return d, date_fmts + dt_fmts
    return None, []


def detect_timestamp_format(df: DataFrame, txt_col: str, formats: list[str]) -> tuple[str | None, dict[str, int]]:
    """Pick the format that parses the most rows (ties -> config order). One Spark job."""
    if not formats:
        return None, {}
    row = df.agg(*[F.count(F.to_timestamp(F.col(txt_col), f)).alias(f"f{i}")
                   for i, f in enumerate(formats)]).first()
    counts = {f: int(row[f"f{i}"] or 0) for i, f in enumerate(formats)}
    best = max(formats, key=lambda f: (counts[f], -formats.index(f)))
    return (best if counts[best] > 0 else None), counts


def clean(raw: RawDataset, mapping: ColumnMapping, settings: dict[str, Any]) -> CleanResult:
    proc = settings.get("processing", {})
    ds_cfg = settings.get("dataset", {})
    sentinels = [float(s) for s in ds_cfg.get("missing_sentinels", []) or []]
    decimal = raw.dialect.decimal if raw.dialect else "."
    o2s = raw.safe_by_original
    df = raw.df

    rows_loaded = df.count()

    # 1) Blank rows ---------------------------------------------------------
    not_blank = _any([(F.col(c).isNotNull() & (F.trim(F.col(c)) != "")) for c in df.columns])
    df = df.filter(not_blank)

    # 2) Parse and validate each measurement --------------------------------
    exprs: list[Column] = []
    flag_aggs: list[Column] = []
    meas_keys = list(mapping.measurements)
    for key in meas_keys:
        meta = mapping.meta[key]
        src = F.col(o2s[meta["source_column"]])
        txt = F.trim(src)
        val = parse_numeric(src, decimal)
        empty = txt.isNull() | (txt == "")
        unparse = (~empty) & val.isNull()
        sentinel = val.isin(sentinels) if sentinels else F.lit(False)
        lo = meta.get("valid_min")
        hi = meta.get("valid_max")
        below = (~sentinel) & val.isNotNull() & (val < F.lit(lo)) if lo is not None else F.lit(False)
        above = (~sentinel) & val.isNotNull() & (val > F.lit(hi)) if hi is not None else F.lit(False)
        ok = val.isNotNull() & ~sentinel & ~below & ~above
        exprs.append(F.when(ok, val).alias(key))
        for name, cond in [("empty", empty), ("unparseable", unparse), ("sentinel", sentinel),
                           ("below_min", below), ("above_max", above), ("valid", ok)]:
            flag_aggs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"{key}__{name}"))

    # 3) Timestamp ------------------------------------------------------------
    ts_txt, formats = _timestamp_candidates(mapping, o2s, proc)
    has_time = ts_txt is not None
    base_cols = [F.col(c) for c in df.columns]
    staged = df.select(*base_cols, *([ts_txt.alias("__ts_txt")] if has_time else []))
    ts_format, fmt_counts = (detect_timestamp_format(staged, "__ts_txt", formats)
                             if has_time else (None, {}))
    if has_time and ts_format:
        # Fallback only to unambiguous ISO formats (year first) so day/month order
        # can never be silently swapped.
        iso = [f for f in formats if f.startswith("yyyy") and f != ts_format and fmt_counts.get(f, 0) > 0]
        ts_expr = F.coalesce(*[F.to_timestamp(F.col("__ts_txt"), f) for f in [ts_format] + iso])
    else:
        ts_expr = F.lit(None).cast("timestamp")

    # 4) Location ---------------------------------------------------------------
    has_loc = "location" in mapping.roles
    unspecified = proc.get("unspecified_location_label", "(unspecified)")
    if has_loc:
        lraw = F.trim(F.col(o2s[mapping.roles["location"]]))
        loc_expr = F.when(lraw.isNull() | (lraw == ""), F.lit(unspecified)).otherwise(lraw)

    parsed = staged.select(
        ts_expr.alias(TS),
        *([loc_expr.alias(LOC)] if has_loc else []),
        *exprs,
    )
    # Flag counts need the raw values, so compute them on `staged` in one pass.
    flag_row = staged.agg(
        F.count(F.lit(1)).alias("__rows"),
        *([F.sum(F.when(ts_expr.isNull(), 1).otherwise(0)).alias("__no_ts")] if has_time else []),
        *([F.sum(F.when(F.trim(F.col(o2s[mapping.roles["location"]])).isNull() |
                        (F.trim(F.col(o2s[mapping.roles["location"]])) == ""), 1).otherwise(0)).alias("__no_loc")]
          if has_loc else []),
        *flag_aggs,
    ).first()
    rows_nonblank = int(flag_row["__rows"] or 0)
    rows_no_ts = int(flag_row["__no_ts"] or 0) if has_time else 0
    rows_no_loc = int(flag_row["__no_loc"] or 0) if has_loc else 0

    parsed = parsed.cache()
    try:
        # 5) Row-level exclusions ------------------------------------------------
        cur = parsed
        excluded_no_ts = 0
        if has_time and proc.get("require_timestamp", True):
            cur = cur.filter(F.col(TS).isNotNull())
            excluded_no_ts = rows_no_ts
        before = cur.count()
        any_valid = _any([F.col(k).isNotNull() for k in meas_keys])
        cur = cur.filter(any_valid)
        after_valid = cur.count()
        excluded_no_meas = before - after_valid

        # 6) Duplicates --------------------------------------------------------------
        deduped = cur.dropDuplicates()
        rows_final = deduped.count()
        exact_dupes = after_valid - rows_final
        key_cols = [c for c in [TS, LOC] if c in deduped.columns and (c != TS or has_time)]
        key_dupes = 0
        if key_cols and rows_final:
            kd = (deduped.groupBy(*key_cols).count().filter(F.col("count") > 1)
                  .agg(F.sum(F.col("count") - 1).alias("d")).first())
            key_dupes = int(kd["d"] or 0) if kd else 0

        final = deduped.orderBy(*[c for c in [TS, LOC] if c in deduped.columns]) if has_time else deduped

        # Final per-measurement availability and period ---------------------------
        fin_aggs = [F.count(F.col(k)).alias(k) for k in meas_keys]
        if has_time:
            fin_aggs += [F.min(TS).alias("__start"), F.max(TS).alias("__end")]
        if has_loc:
            fin_aggs.append(F.countDistinct(LOC).alias("__nloc"))
        fin = final.agg(*fin_aggs).first() if fin_aggs else None
    finally:
        pass  # `parsed` stays cached until the pipeline unpersists or the session ends

    columns_report = {}
    for key in meas_keys:
        meta = mapping.meta[key]
        valid_final = int(fin[key] or 0) if fin is not None else 0
        columns_report[key] = {
            "source_column": meta["source_column"], "label": meta["label"], "name": meta["name"],
            "unit": meta["unit"], "kind": meta["kind"],
            "valid_min": meta.get("valid_min"), "valid_max": meta.get("valid_max"),
            **{n: int(flag_row[f"{key}__{n}"] or 0) for n in
               ["empty", "unparseable", "sentinel", "below_min", "above_max", "valid"]},
            "valid_in_final_dataset": valid_final,
            "pct_valid_in_final_dataset": round(100.0 * valid_final / rows_final, 2) if rows_final else 0.0,
        }

    report: dict[str, Any] = {
        "source_file": raw.source_path.name,
        "file_format": raw.file_format,
        "csv_delimiter": raw.dialect.delimiter if raw.dialect else None,
        "decimal_separator": decimal,
        "missing_sentinels": sentinels,
        "source_columns": list(raw.original_names.values()),
        "dropped_empty_unnamed_columns": len(raw.dropped_empty_columns),
        "column_mapping": mapping.to_dict(),
        "timestamp": {
            "available": has_time,
            "detected_format": ts_format,
            "format_parse_counts": {k: v for k, v in fmt_counts.items() if v},
            "interpreted_as": "local wall-clock time (stored as UTC, no zone conversion)",
        },
        "rows": {
            "loaded": rows_loaded,
            "blank_rows_removed": rows_loaded - rows_nonblank,
            "without_parseable_timestamp": rows_no_ts,
            "excluded_without_timestamp": excluded_no_ts,
            "excluded_without_any_valid_measurement": excluded_no_meas,
            "exact_duplicates_removed": exact_dupes,
            "duplicate_timestamp_location_keys_kept": key_dupes,
            "location_missing_labelled_unspecified": rows_no_loc,
            "retained": rows_final,
        },
        "measurements": columns_report,
        "period": {
            "start": str(fin["__start"]) if has_time and fin is not None and fin["__start"] else None,
            "end": str(fin["__end"]) if has_time and fin is not None and fin["__end"] else None,
        },
        "n_locations": int(fin["__nloc"]) if has_loc and fin is not None else (1 if rows_final else 0),
        "has_location": has_loc,
        "has_timestamp": has_time,
    }
    return CleanResult(df=final, report=report)
