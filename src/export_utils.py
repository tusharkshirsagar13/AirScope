"""Export helpers: CSV downloads and the JSON analysis report."""
from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

LIMITATIONS = [
    "Results describe only the supplied dataset and the filters applied; they are not a "
    "regulatory assessment or medical advice.",
    "Statistical outliers are unusual values relative to this dataset, not official pollution alerts.",
    "Correlation indicates association between measurements, not causation.",
    "No AQI is calculated unless the dataset supplies one; pollutant concentrations are analysed directly.",
    "Measurements with different units are never plotted on one numeric scale; comparisons across "
    "pollutants use values indexed to each pollutant's own mean.",
    "Spark ran in local mode on one machine; timings do not represent a multi-node cluster.",
]


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _jsonable(o: Any) -> Any:
    if isinstance(o, pd.DataFrame):
        return [_jsonable(r) for r in o.to_dict(orient="records")]
    if isinstance(o, pd.Series):
        return _jsonable(o.to_dict())
    if isinstance(o, dict):
        return {str(k): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, (pd.Timestamp, datetime, date)):
        return o.isoformat()
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating, float)):
        f = float(o)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 6)
    if o is pd.NaT:
        return None
    return o


def build_report(*, dataset: dict[str, Any], processing: dict[str, Any], filters: dict[str, Any],
                 results: dict[str, Any], observations: list[str]) -> dict[str, Any]:
    return _jsonable({
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "project": "AirScope — Air Quality and Pollution Data Analytics Using Big Data Processing",
        "dataset": dataset,
        "processing": processing,
        "filters": filters,
        "results": results,
        "observations": observations,
        "limitations": LIMITATIONS,
    })


def report_to_json_bytes(report: dict[str, Any]) -> bytes:
    return json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8")


def save_report(report: dict[str, Any], out_dir: Path, name: str = "airscope_report.json") -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_bytes(report_to_json_bytes(report))
    return path
