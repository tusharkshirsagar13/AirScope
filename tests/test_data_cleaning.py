"""Cleaning rules: numeric conversion, missing codes, ranges, timestamps, duplicates."""
from __future__ import annotations

import datetime as dt
import hashlib

from pyspark.sql import functions as F

from src.data_cleaning import clean, parse_numeric
from src.data_loader import load_raw, map_columns
from src.spark_processor import process_dataset


def _clean(spark, path, settings, mapping):
    raw = load_raw(spark, path, settings)
    cm = map_columns(list(raw.original_names.values()), mapping)
    return clean(raw, cm, settings)


def test_parse_numeric_comma_and_dot(spark):
    df = spark.createDataFrame([(" 2,6 ",), ("-1,5",), ("abc",), ("NaN",), ("",), (None,)], ["v"])
    out = [r[0] for r in df.select(parse_numeric(F.col("v"), ",")).collect()]
    assert out == [2.6, -1.5, None, None, None, None]
    df2 = spark.createDataFrame([("3.25",), ("1e3",)], ["v"])
    assert [r[0] for r in df2.select(parse_numeric(F.col("v"), ".")).collect()] == [3.25, 1000.0]


def test_cleaning_report_counts(spark, settings, mapping, uci_like):
    r = _clean(spark, uci_like, settings, mapping)
    rows = r.report["rows"]
    assert rows["loaded"] == 9
    assert rows["blank_rows_removed"] == 1
    assert rows["without_parseable_timestamp"] == 1
    assert rows["excluded_without_timestamp"] == 1
    assert rows["excluded_without_any_valid_measurement"] == 1
    assert rows["exact_duplicates_removed"] == 1
    assert rows["retained"] == 5

    co = r.report["measurements"]["co"]
    assert co["sentinel"] == 2          # two -200 values (rows 3 and 7)
    assert co["unparseable"] == 1       # "abc"
    assert co["above_max"] == 2         # 80 mg/m3 twice (before de-duplication)
    rh = r.report["measurements"]["rh"]
    assert rh["above_max"] == 2         # RH 150 %


def test_negative_temperature_is_valid(spark, settings, mapping, uci_like):
    r = _clean(spark, uci_like, settings, mapping)
    temps = sorted(x["temperature"] for x in r.df.select("temperature").collect() if x["temperature"] is not None)
    assert -1.5 in temps
    assert r.report["measurements"]["temperature"]["below_min"] == 0


def test_invalid_values_become_null_not_imputed(spark, settings, mapping, uci_like):
    r = _clean(spark, uci_like, settings, mapping)
    row = r.df.filter(F.col("ts") == dt.datetime(2004, 3, 10, 22)).first()
    assert row["co"] is None and row["rh"] is None   # out of range -> excluded, not clipped
    assert row["no2"] == 131.0                        # other values in the row are kept


def test_timestamp_parsing_dd_mm(spark, settings, mapping, uci_like):
    r = _clean(spark, uci_like, settings, mapping)
    assert r.report["timestamp"]["detected_format"] == "dd/MM/yyyy HH.mm.ss"
    first = r.df.orderBy("ts").first()["ts"]
    assert first == dt.datetime(2004, 3, 10, 18, 0)   # 10 March, not 3 October


def test_timestamp_iso_datetime_column(spark, settings, mapping, write):
    p = write("iso.csv", "timestamp,pm25\n2024-01-31 10:00:00,5\n2024-02-01T11:00:00,6\nnot a date,7\n")
    r = _clean(spark, p, settings, mapping)
    assert r.report["rows"]["retained"] == 2
    assert sorted(x["ts"] for x in r.df.collect()) == [dt.datetime(2024, 1, 31, 10), dt.datetime(2024, 2, 1, 11)]


def test_us_date_format_detected(spark, settings, mapping, write):
    p = write("us.csv", "date,no2\n01/31/2024,10\n02/28/2024,20\n")
    r = _clean(spark, p, settings, mapping)
    assert r.report["timestamp"]["detected_format"].startswith("MM/dd/yyyy")
    assert r.report["rows"]["retained"] == 2


def test_missing_location_labelled(spark, settings, mapping, write):
    p = write("loc.csv", "date,city,pm10\n2024-01-01,Pune,10\n2024-01-01,,20\n")
    r = _clean(spark, p, settings, mapping)
    locs = sorted(x["location"] for x in r.df.collect())
    assert locs == ["(unspecified)", "Pune"]
    assert r.report["rows"]["location_missing_labelled_unspecified"] == 1


def test_duplicate_keys_reported_but_kept(spark, settings, mapping, write):
    p = write("dupkey.csv", "date,city,pm10\n2024-01-01,Pune,10\n2024-01-01,Pune,12\n")
    r = _clean(spark, p, settings, mapping)
    assert r.report["rows"]["retained"] == 2
    assert r.report["rows"]["duplicate_timestamp_location_keys_kept"] == 1


def test_raw_file_never_modified_and_cache_reused(spark, settings, mapping, uci_like):
    before = hashlib.sha256(uci_like.read_bytes()).hexdigest()
    first = process_dataset(spark, uci_like, settings, mapping)
    second = process_dataset(spark, uci_like, settings, mapping)
    assert hashlib.sha256(uci_like.read_bytes()).hexdigest() == before
    assert first.persisted and not first.from_cache
    assert second.from_cache and second.df.count() == first.report["rows"]["retained"]
    forced = process_dataset(spark, uci_like, settings, mapping, force=True)
    assert not forced.from_cache
