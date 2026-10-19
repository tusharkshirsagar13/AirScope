"""Correctness of the Spark analytics, insights, exports and app start-up."""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path

import pandas as pd
import pytest

from src import analytics as A
from src.export_utils import build_report, df_to_csv_bytes, report_to_json_bytes
from src.insights import generate_observations


@pytest.fixture(scope="module")
def df(spark):
    """48 hourly rows over 2 days (Fri 5 Jan & Sat 6 Jan 2024), two stations.
    a = hour of day, b = 2a (perfectly correlated), c = -a, d mostly null."""
    rows = []
    for day in (5, 6):
        for h in range(24):
            for loc, off in (("North", 0.0), ("South", 10.0)):
                a = float(h) + off
                rows.append((dt.datetime(2024, 1, day, h), loc, a, 2 * a, -a, 1.0 if h < 3 else None))
    return spark.createDataFrame(rows, "ts timestamp, location string, a double, b double, c double, d double")


def test_overview_and_filters(df):
    ov = A.overview(df)
    assert ov["rows"] == 96 and ov["locations"] == 2
    f = A.apply_filters(df, dt.date(2024, 1, 6), dt.date(2024, 1, 6), ["South"])
    assert A.overview(f)["rows"] == 24          # end date is inclusive
    assert A.overview(A.apply_filters(df, dt.date(2024, 2, 1), None))["rows"] == 0


def test_summary_stats_exact(df):
    s = A.summary_stats(df, ["a", "d"]).set_index("measurement")
    assert s.loc["a", "count"] == 96
    assert s.loc["a", "mean"] == pytest.approx(16.5)      # mean of 0..23 is 11.5, +5 avg offset
    assert s.loc["a", "min"] == 0 and s.loc["a", "max"] == 33
    assert s.loc["d", "count"] == 12                       # nulls are excluded, not zero-filled


def test_time_series_daily(df):
    ts = A.time_series(df, "a", "day")
    assert list(ts["count"]) == [48, 48]
    assert ts["mean"].tolist() == pytest.approx([16.5, 16.5])


def test_profiles(df):
    hp = A.profile(df, "a", "hour")
    assert len(hp) == 24 and hp.loc[hp["key"] == 0, "mean"].item() == pytest.approx(5.0)
    wk = A.profile(df, "a", "weekday")
    assert wk["label"].tolist() == ["Friday", "Saturday"]
    se = A.profile(df, "a", "season")
    assert se["label"].tolist() == ["Winter"] and se["count"].item() == 96
    wv = A.weekday_vs_weekend(df, "a")
    assert wv["n_weekday"] == 48 and wv["n_weekend"] == 48


def test_location_comparison_sorted(df):
    lc = A.location_comparison(df, "a")
    assert lc["location"].tolist() == ["South", "North"]
    assert lc["mean"].tolist() == pytest.approx([21.5, 11.5])


def test_correlation(df):
    corr, n = A.correlation_matrix(df, ["a", "b", "c", "d"], min_obs=30)
    assert corr.loc["a", "b"] == pytest.approx(1.0)
    assert corr.loc["a", "c"] == pytest.approx(-1.0)
    assert math.isnan(corr.loc["a", "d"])        # only 12 joint observations < 30
    assert n.loc["a", "d"] == 12


def test_outliers_iqr(spark):
    vals = [float(v) for v in range(1, 21)] + [100.0]
    d = spark.createDataFrame([(dt.datetime(2024, 1, 1, i % 24), v) for i, v in enumerate(vals)], "ts timestamp, x double")
    o = A.outlier_analysis(d, "x", method="iqr", k=1.5)
    assert o["count"] == 1 and o["top"]["x"].iloc[0] == 100.0
    assert o["threshold"] < 100
    p = A.outlier_analysis(d, "x", method="percentile", percentile=0.9)
    assert p["count"] >= 1


def test_histogram_counts_sum(df):
    h = A.histogram(df, "a", bins=10)
    assert len(h) == 10 and h["count"].sum() == 96
    assert h["bin_start"].iloc[0] == 0


def test_top_periods_requires_coverage(spark):
    rows = [(dt.datetime(2024, 1, 1, h), 1.0) for h in range(24)] + [(dt.datetime(2024, 1, 2, 0), 99.0)]
    d = spark.createDataFrame(rows, "ts timestamp, x double")
    tp = A.top_periods(d, "x", "day", n=5)
    assert len(tp) == 1 and tp["mean"].iloc[0] == 1.0     # 1-reading day is not ranked


def test_completeness_and_indexed(df):
    c = A.completeness(df, ["a", "d"])
    assert c["a"].iloc[0] == 100.0 and c["d"].iloc[0] == pytest.approx(12.5)
    idx = A.indexed_monthly(df, ["a"])
    assert idx["index"].iloc[0] == pytest.approx(100.0)


def test_sample_is_bounded(df):
    s = A.sample_records(df, ["a"], limit=5)
    assert len(s) == 5 and s["ts"].iloc[0] == pd.Timestamp("2024-01-06 23:00")


def test_observations_are_data_driven(df):
    ov = A.overview(df)
    stats = A.summary_stats(df, ["a"]).iloc[0]
    corr, _ = A.correlation_matrix(df, ["a", "b"])
    obs = generate_observations(label="A", unit="µg/m³", overview=ov, stats=stats,
                                hourly=A.profile(df, "a", "hour"), corr=corr,
                                locations=A.location_comparison(df, "a"))
    text = " ".join(obs)
    assert "96 records" in text and "South" in text and "23:00" in text
    assert "does not show that one causes the other" in text
    # deterministic
    assert obs == generate_observations(label="A", unit="µg/m³", overview=ov, stats=stats,
                                        hourly=A.profile(df, "a", "hour"), corr=corr,
                                        locations=A.location_comparison(df, "a"))
    assert generate_observations(label="A", unit="", overview={"rows": 0})[0].startswith("No records")


def test_exports(df):
    s = A.summary_stats(df, ["a"])
    csv = df_to_csv_bytes(s).decode()
    assert csv.splitlines()[0].startswith("measurement,count,mean")
    rep = build_report(dataset={"file": "x"}, processing={}, filters={"start": dt.date(2024, 1, 1)},
                       results={"stats": s, "nan": float("nan")}, observations=["o"])
    parsed = json.loads(report_to_json_bytes(rep))
    assert parsed["filters"]["start"] == "2024-01-01"
    assert parsed["results"]["nan"] is None and parsed["limitations"]


def test_app_starts_with_real_dataset():
    """Runs app.py headlessly with Streamlit's AppTest (skipped without the real dataset)."""
    root = Path(__file__).resolve().parents[1]
    if not (root / "data" / "raw" / "AirQuality.csv").exists():
        pytest.skip("real dataset not present")
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file(str(root / "app.py"), default_timeout=600)
    at.run()
    assert not at.exception, at.exception
    assert len(at.get("plotly_chart")) >= 5
