"""Pandas vs Spark benchmark on the cleaned Parquet output.

    python scripts/benchmark.py                 # original rows
    python scripts/benchmark.py --scale 50      # rows replicated 50x for timing only

Run scripts/run_pipeline.py (or open the dashboard once) first so the cleaned
Parquet exists. See src/benchmark.py for the methodology and caveats.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.benchmark import run_benchmark  # noqa: E402
from src.config import load_mapping, load_settings, resolve_path  # noqa: E402
from src.data_loader import get_spark  # noqa: E402
from src.spark_processor import process_dataset  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset")
    ap.add_argument("--scale", type=int, default=1)
    ap.add_argument("--repeats", type=int, default=3)
    a = ap.parse_args()
    settings, mapping = load_settings(), load_mapping()
    spark = get_spark(settings)
    p = process_dataset(spark, resolve_path(a.dataset or settings["dataset"]["path"]), settings, mapping)
    if not p.persisted or not p.has_time:
        print("Benchmark needs the cleaned Parquet output and a timestamp column.")
        return 1
    cols = [k for k, m in p.measurements.items() if m["valid_in_final_dataset"] > 0]
    res = run_benchmark(spark, p.output_dir / "clean.parquet", cols, repeats=a.repeats, scale=a.scale)
    print(json.dumps(res, indent=2))
    out = resolve_path(settings["paths"]["reports_dir"])
    out.mkdir(parents=True, exist_ok=True)
    (out / f"benchmark_x{a.scale}.json").write_text(json.dumps(res, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
