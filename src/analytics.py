"""Analytical queries, all executed by Spark.

Every function receives the (filtered) Spark DataFrame and returns a *small*
pandas DataFrame or dict — an aggregate, a profile or a bounded sample — that
is cheap to plot. The full dataset is never collected to the driver.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from itertools import combinations
from typing import Any, Iterable

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .data_cleaning import LOC, TS

WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
SEASONS_NORTH = {12: "Winter", 1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring", 5: "Spring",
                 6: "Summer", 7: "Summer", 8: "Summer", 9: "Autumn", 10: "Autumn", 11: "Autumn"}
SEASONS_SOUTH = {m: {"Winter": "Summer", "Summer": "Winter", "Spring": "Autumn", "Autumn": "Spring"}[s]
                 for m, s in SEASONS_NORTH.items()}
SEASON_ORDER = ["Winter", "Spring", "Summer", "Autumn"]
GRANULARITIES = {"Hourly": "hour", "Daily": "day", "Weekly": "week", "Monthly": "month"}


# --------------------------------------------------------------------------- #
# Filtering
# --------------------------------------------------------------------------- #
def apply_filters(df: DataFrame, start: date | None = None, end: date | None = None,
                  locations: Iterable[str] | None = None) -> DataFrame:
    """Date range is inclusive of both ends. Year predicates allow Parquet partition pruning."""
    if start is not None and TS in df.columns:
        df = df.filter(F.col(TS) >= F.lit(datetime.combine(start, datetime.min.time())))
        if "year" in df.columns:
            df = df.filter(F.col("year") >= start.year)
    if end is not None and TS in df.columns:
        df = df.filter(F.col(TS) < F.lit(datetime.combine(end + timedelta(days=1), datetime.min.time())))
        if "year" in df.columns:
            df = df.filter(F.col("year") <= end.year)
    if locations and LOC in df.columns:
        df = df.filter(F.col(LOC).isin(list(locations)))
    return df


# --------------------------------------------------------------------------- #
# Overview & descriptive statistics
# --------------------------------------------------------------------------- #
def overview(df: DataFrame) -> dict[str, Any]:
    aggs = [F.count(F.lit(1)).alias("rows")]
    if TS in df.columns:
        aggs += [F.min(TS).alias("start"), F.max(TS).alias("end")]
    if LOC in df.columns:
        aggs.append(F.countDistinct(LOC).alias("locations"))
    r = df.agg(*aggs).first()
    out = {"rows": int(r["rows"])}
    if TS in df.columns:
        out["start"], out["end"] = r["start"], r["end"]
    out["locations"] = int(r["locations"]) if LOC in df.columns else (1 if out["rows"] else 0)
    return out


def summary_stats(df: DataFrame, cols: list[str], accuracy: int = 10000) -> pd.DataFrame:
    """count / mean / std / min / quartiles (approximate) / max for each column — one Spark job."""
    if not cols:
        return pd.DataFrame()
    aggs = []
    for c in cols:
        aggs += [F.count(c).alias(f"{c}__count"), F.avg(c).alias(f"{c}__mean"),
                 F.stddev_samp(c).alias(f"{c}__std"), F.min(c).alias(f"{c}__min"),
                 F.max(c).alias(f"{c}__max"),
                 F.percentile_approx(c, [0.25, 0.5, 0.75], accuracy).alias(f"{c}__q")]
    r = df.agg(*aggs).first()
    rows = []
    for c in cols:
        q = r[f"{c}__q"] or [None, None, None]
        rows.append({"measurement": c, "count": int(r[f"{c}__count"] or 0),
                     "mean": r[f"{c}__mean"], "std": r[f"{c}__std"], "min": r[f"{c}__min"],
                     "q1": q[0], "median": q[1], "q3": q[2], "max": r[f"{c}__max"]})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Time-based analysis
# --------------------------------------------------------------------------- #
def time_series(df: DataFrame, col: str, granularity: str = "day", max_points: int = 20000) -> pd.DataFrame:
    """Mean / min / max / count of `col` per period (date_trunc + groupBy)."""
    period = F.date_trunc(granularity, F.col(TS))
    out = (df.filter(F.col(col).isNotNull()).groupBy(period.alias("period"))
           .agg(F.avg(col).alias("mean"), F.min(col).alias("min"), F.max(col).alias("max"),
                F.count(col).alias("count"))
           .orderBy("period").limit(max_points))
    return out.toPandas()


def time_series_by_location(df: DataFrame, col: str, locations: list[str], granularity: str = "day") -> pd.DataFrame:
    period = F.date_trunc(granularity, F.col(TS))
    return (df.filter(F.col(col).isNotNull() & F.col(LOC).isin(locations))
            .groupBy(period.alias("period"), LOC).agg(F.avg(col).alias("mean"))
            .orderBy("period").toPandas())


def profile(df: DataFrame, col: str, by: str, hemisphere: str = "north") -> pd.DataFrame:
    """Average pattern of `col` by hour of day, weekday, calendar month or season."""
    base = df.filter(F.col(col).isNotNull())
    if by == "hour":
        key = F.hour(TS)
    elif by == "weekday":
        key = ((F.dayofweek(TS) + 5) % 7)           # Monday=0 ... Sunday=6
    elif by in ("month", "season"):
        key = F.month(TS)
    else:
        raise ValueError(f"Unknown profile '{by}'")
    pdf = (base.groupBy(key.alias("key"))
           .agg(F.avg(col).alias("mean"), F.stddev_samp(col).alias("std"), F.count(col).alias("count"))
           .orderBy("key").toPandas())
    if pdf.empty:
        return pdf.assign(label=[])
    if by == "hour":
        pdf["label"] = pdf["key"].map(lambda h: f"{int(h):02d}:00")
    elif by == "weekday":
        pdf["label"] = pdf["key"].map(lambda d: WEEKDAYS[int(d)])
    elif by == "month":
        pdf["label"] = pdf["key"].map(lambda m: MONTHS[int(m) - 1])
    else:  # season: combine months with count-weighted mean (exact, no re-scan)
        seasons = SEASONS_NORTH if hemisphere == "north" else SEASONS_SOUTH
        pdf["label"] = pdf["key"].map(lambda m: seasons[int(m)])
        pdf["sum"] = pdf["mean"] * pdf["count"]
        g = pdf.groupby("label", as_index=False).agg(sum=("sum", "sum"), count=("count", "sum"),
                                                      months=("key", lambda s: ", ".join(MONTHS[int(m) - 1] for m in sorted(s))))
        g["mean"] = g["sum"] / g["count"]
        g["key"] = g["label"].map(SEASON_ORDER.index)
        pdf = g.sort_values("key")[["key", "label", "mean", "count", "months"]].reset_index(drop=True)
    return pdf


def weekday_vs_weekend(df: DataFrame, col: str) -> dict[str, Any]:
    weekend = F.dayofweek(TS).isin(1, 7)
    r = (df.filter(F.col(col).isNotNull())
         .agg(F.avg(F.when(~weekend, F.col(col))).alias("weekday"),
              F.avg(F.when(weekend, F.col(col))).alias("weekend"),
              F.count(F.when(~weekend, F.col(col))).alias("n_weekday"),
              F.count(F.when(weekend, F.col(col))).alias("n_weekend")).first())
    return {k: r[k] for k in ["weekday", "weekend", "n_weekday", "n_weekend"]}


def top_periods(df: DataFrame, col: str, granularity: str = "day", n: int = 10,
                min_coverage: float = 0.75) -> pd.DataFrame:
    """Periods with the highest mean. A "full" period is taken as the 90th
    percentile of observations-per-period; periods with fewer than
    `min_coverage` × that number are skipped, so a day with only one or two
    readings cannot top the list."""
    period = F.date_trunc(granularity, F.col(TS))
    g = (df.filter(F.col(col).isNotNull()).groupBy(period.alias("period"))
         .agg(F.avg(col).alias("mean"), F.max(col).alias("max"), F.count(col).alias("count")))
    med = g.agg(F.percentile_approx("count", 0.9).alias("m")).first()["m"]
    if med is None:
        return pd.DataFrame(columns=["period", "mean", "max", "count"])
    threshold = max(1, int(math.floor(med * min_coverage)))
    return (g.filter(F.col("count") >= threshold).orderBy(F.desc("mean")).limit(n).toPandas()
            .assign(min_obs_required=threshold))


def completeness(df: DataFrame, cols: list[str], granularity: str = "month") -> pd.DataFrame:
    """% of rows with a valid value for each measurement, per period."""
    period = F.date_trunc(granularity, F.col(TS))
    aggs = [F.count(F.lit(1)).alias("rows")] + [F.count(c).alias(c) for c in cols]
    pdf = df.groupBy(period.alias("period")).agg(*aggs).orderBy("period").toPandas()
    for c in cols:
        pdf[c] = (100.0 * pdf[c] / pdf["rows"]).round(1)
    return pdf


def indexed_monthly(df: DataFrame, cols: list[str]) -> pd.DataFrame:
    """Monthly mean of each measurement expressed as % of its own overall mean.

    Different pollutants use different units, so raw values cannot share an
    axis; indexing to 100 compares their *relative* seasonal movement only."""
    overall = df.agg(*[F.avg(c).alias(c) for c in cols]).first()
    monthly = (df.groupBy(F.date_trunc("month", F.col(TS)).alias("period"))
               .agg(*[F.avg(c).alias(c) for c in cols], *[F.count(c).alias(f"{c}__n") for c in cols])
               .orderBy("period").toPandas())
    rows = []
    for c in cols:
        base = overall[c]
        if base in (None, 0):
            continue
        for _, r in monthly.iterrows():
            if pd.notna(r[c]):
                rows.append({"period": r["period"], "measurement": c,
                             "index": 100.0 * r[c] / base, "mean": r[c], "count": int(r[f"{c}__n"])})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Location comparison
# --------------------------------------------------------------------------- #
def location_comparison(df: DataFrame, col: str, limit: int = 500) -> pd.DataFrame:
    return (df.filter(F.col(col).isNotNull()).groupBy(LOC)
            .agg(F.avg(col).alias("mean"), F.max(col).alias("max"), F.min(col).alias("min"),
                 F.stddev_samp(col).alias("std"), F.count(col).alias("count"))
            .orderBy(F.desc("mean")).limit(limit).toPandas())


# --------------------------------------------------------------------------- #
# Relationships and unusual values
# --------------------------------------------------------------------------- #
def correlation_matrix(df: DataFrame, cols: list[str], min_obs: int = 30) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pearson correlation for every pair using pairwise-complete observations.

    All pairs are computed in ONE Spark aggregation (`corr` skips rows where
    either value is null). Pairs with fewer than `min_obs` joint observations
    are reported as NaN."""
    if len(cols) < 2:
        return pd.DataFrame(), pd.DataFrame()
    pairs = list(combinations(cols, 2))
    aggs = []
    for i, (a, b) in enumerate(pairs):
        aggs.append(F.corr(a, b).alias(f"r{i}"))
        aggs.append(F.count(F.when(F.col(a).isNotNull() & F.col(b).isNotNull(), 1)).alias(f"n{i}"))
    r = df.agg(*aggs).first()
    corr = pd.DataFrame(1.0, index=cols, columns=cols)
    nobs = pd.DataFrame(0, index=cols, columns=cols)
    for c in cols:
        nobs.loc[c, c] = None
    for i, (a, b) in enumerate(pairs):
        n = int(r[f"n{i}"] or 0)
        v = r[f"r{i}"]
        val = float(v) if (v is not None and n >= min_obs and not math.isnan(v)) else float("nan")
        corr.loc[a, b] = corr.loc[b, a] = val
        nobs.loc[a, b] = nobs.loc[b, a] = n
    return corr, nobs


def outlier_analysis(df: DataFrame, col: str, method: str = "iqr", k: float = 1.5,
                     percentile: float = 0.99, accuracy: int = 10000, top_n: int = 10) -> dict[str, Any]:
    """Flag unusually HIGH values of `col`.

    iqr:        threshold = Q3 + k·(Q3 − Q1)   (Tukey's fence)
    percentile: threshold = the chosen percentile (e.g. P99)

    These are statistical outliers relative to this dataset — NOT official
    pollution alerts, which depend on legal limits and averaging periods."""
    base = df.filter(F.col(col).isNotNull())
    q = base.agg(F.percentile_approx(col, [0.25, 0.75, percentile], accuracy).alias("q"),
                 F.count(col).alias("n")).first()
    n = int(q["n"] or 0)
    if n == 0 or q["q"] is None:
        return {"method": method, "n": 0, "threshold": None, "count": 0, "share_pct": 0.0,
                "top": pd.DataFrame(), "by_month": pd.DataFrame()}
    q1, q3, p = q["q"]
    if method == "percentile":
        threshold = p
        rule = f"value > P{percentile * 100:g} ({p:.4g})"
    else:
        threshold = q3 + k * (q3 - q1)
        rule = f"value > Q3 + {k:g}×IQR = {q3:.4g} + {k:g}×{q3 - q1:.4g}"
    flagged = base.filter(F.col(col) > F.lit(threshold))
    count = flagged.count()
    keep = [c for c in [TS, LOC, col] if c in df.columns]
    top = flagged.select(*keep).orderBy(F.desc(col)).limit(top_n).toPandas()
    by_month = pd.DataFrame()
    if TS in df.columns:
        by_month = (base.groupBy(F.date_trunc("month", F.col(TS)).alias("period"))
                    .agg(F.count(F.when(F.col(col) > F.lit(threshold), 1)).alias("outliers"),
                         F.count(col).alias("observations"))
                    .orderBy("period").toPandas())
        by_month["share_pct"] = 100.0 * by_month["outliers"] / by_month["observations"]
    by_hour = pd.DataFrame()
    if TS in df.columns and count:
        by_hour = (flagged.groupBy(F.hour(TS).alias("hour")).count().orderBy("hour").toPandas())
    return {"method": method, "rule": rule, "q1": q1, "q3": q3, "n": n, "threshold": threshold,
            "count": count, "share_pct": 100.0 * count / n, "top": top, "by_month": by_month,
            "by_hour": by_hour}


def histogram(df: DataFrame, col: str, bins: int = 40) -> pd.DataFrame:
    """Equal-width histogram computed with a Spark groupBy (no raw data collected)."""
    mm = df.agg(F.min(col).alias("lo"), F.max(col).alias("hi")).first()
    lo, hi = mm["lo"], mm["hi"]
    if lo is None:
        return pd.DataFrame(columns=["bin_start", "bin_end", "count"])
    if hi == lo:
        return pd.DataFrame({"bin_start": [lo], "bin_end": [hi], "count": [df.filter(F.col(col).isNotNull()).count()]})
    width = (hi - lo) / bins
    b = F.least(F.floor((F.col(col) - F.lit(lo)) / F.lit(width)).cast("int"), F.lit(bins - 1))
    pdf = df.filter(F.col(col).isNotNull()).groupBy(b.alias("b")).count().toPandas()
    full = pd.DataFrame({"b": range(bins)}).merge(pdf, on="b", how="left").fillna({"count": 0})
    full["bin_start"] = lo + full["b"] * width
    full["bin_end"] = full["bin_start"] + width
    return full[["bin_start", "bin_end", "count"]].astype({"count": int})


def sample_records(df: DataFrame, cols: list[str], limit: int = 1000, newest_first: bool = True) -> pd.DataFrame:
    keep = [c for c in [TS, LOC] if c in df.columns] + [c for c in cols if c in df.columns]
    out = df.select(*keep)
    if TS in df.columns:
        out = out.orderBy(F.desc(TS) if newest_first else F.asc(TS))
    return out.limit(limit).toPandas()
