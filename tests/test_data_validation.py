"""Loading, format detection, schema/column mapping and edge-case datasets."""
from __future__ import annotations

import json

import pandas as pd
import pytest

from src.data_loader import (DatasetError, detect_format, load_raw, map_columns, normalise_name,
                             safe_column_names, sniff_csv)
from src.spark_processor import process_dataset


# ---------------------------------------------------------------- format sniffing
def test_sniff_semicolon_comma_decimal(uci_like):
    d = sniff_csv(uci_like)
    assert d.delimiter == ";" and d.decimal == ","


def test_sniff_plain_comma_csv(write):
    p = write("a.csv", "date,city,pm25\n2024-01-01,Pune,35.5\n2024-01-02,Pune,40.1\n")
    d = sniff_csv(p)
    assert d.delimiter == "," and d.decimal == "."


def test_unsupported_format(tmp_path):
    with pytest.raises(DatasetError):
        detect_format(tmp_path / "x.docx")


def test_missing_file_raises(spark, settings, mapping, tmp_path):
    with pytest.raises(DatasetError, match="not found"):
        process_dataset(spark, tmp_path / "nope.csv", settings, mapping)


def test_empty_file_raises(spark, settings, write):
    p = write("empty.csv", "")
    with pytest.raises(DatasetError, match="empty"):
        load_raw(spark, p, settings)


def test_header_only_dataset_gives_zero_rows(spark, settings, mapping, write):
    p = write("header_only.csv", "Date,Time,CO\n")
    res = process_dataset(spark, p, settings, mapping)
    assert res.report["rows"]["loaded"] == 0
    assert res.report["rows"]["retained"] == 0
    assert res.persisted is False          # nothing written for an empty result


# ---------------------------------------------------------------- column mapping
def test_normalise_and_safe_names():
    assert normalise_name("PM2.5") == "pm25"
    assert normalise_name("CO(GT)") == "cogt"
    assert safe_column_names(["CO(GT)", "PT08.S1(CO)", "", "CO(GT)"]) == ["co_gt", "pt08_s1_co", "col_2", "co_gt_1"]


def test_map_uci_headers(mapping):
    cols = ["Date", "Time", "CO(GT)", "PT08.S1(CO)", "NMHC(GT)", "C6H6(GT)", "NOx(GT)", "NO2(GT)", "T", "RH", "AH"]
    m = map_columns(cols, mapping)
    assert m.roles == {"date": "Date", "time": "Time"}
    assert m.measurements["co"] == "CO(GT)"
    assert m.measurements["s1_co"] == "PT08.S1(CO)"     # sensor is NOT mistaken for CO concentration
    assert m.measurements["temperature"] == "T"
    assert m.unmapped == []
    assert any("location" in w for w in m.warnings)
    assert any("AQI" in w for w in m.warnings)


def test_map_generic_headers(mapping):
    m = map_columns(["Datetime", "City", "PM2.5", "PM10", "AQI", "Wind"], mapping)
    assert m.roles == {"datetime": "Datetime", "location": "City"}
    assert set(m.measurements) == {"pm25", "pm10", "aqi"}
    assert m.unmapped == ["Wind"]                      # unknown column is reported, not guessed


def test_ambiguous_columns_warn(mapping):
    m = map_columns(["date", "city", "station", "pm25"], mapping)
    assert m.roles["location"] == "city"
    assert any("Several columns match 'location'" in w for w in m.warnings)


def test_no_recognised_columns(mapping):
    m = map_columns(["foo", "bar"], mapping)
    assert not m.measurements and not m.roles
    assert any("No measurement columns" in w for w in m.warnings)


# ---------------------------------------------------------------- other formats
def test_json_and_parquet_and_excel(spark, settings, mapping, tmp_path):
    pdf = pd.DataFrame({"timestamp": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"],
                        "station": ["A", "B"], "PM10": [20.0, 30.0]})
    raw = tmp_path / "raw"
    raw.mkdir()
    (raw / "d.json").write_text(json.dumps(pdf.to_dict(orient="records")), encoding="utf-8")
    pdf.to_parquet(raw / "d.parquet")
    pdf.to_excel(raw / "d.xlsx", index=False)
    for name in ["d.json", "d.parquet", "d.xlsx"]:
        res = process_dataset(spark, raw / name, settings, mapping)
        assert res.report["rows"]["retained"] == 2, name
        assert res.report["n_locations"] == 2
        vals = sorted(r["pm10"] for r in res.df.select("pm10").collect())
        assert vals == [20.0, 30.0]


def test_dataset_without_location_or_aqi(spark, settings, mapping, uci_like):
    res = process_dataset(spark, uci_like, settings, mapping)
    assert res.has_location is False
    assert "location" not in res.df.columns
    assert "aqi" not in res.measurements
    assert res.report["n_locations"] == 1


def test_dataset_without_date_column(spark, settings, mapping, write):
    p = write("nodate.csv", "city,no2\nA,10\nB,20\nB,30\n")
    res = process_dataset(spark, p, settings, mapping)
    assert res.has_time is False
    assert res.report["rows"]["retained"] == 3
    assert res.persisted          # written without year/month partitions
