"""Run the full Spark pipeline from the command line (no dashboard).

    python scripts/run_pipeline.py                      # dataset from config/settings.yaml
    python scripts/run_pipeline.py --dataset data/raw/other.csv --force

Writes to data/processed/ (cleaned Parquet + cleaning report) and to
data/reports/ (summary CSVs and a JSON analysis report).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src import analytics as A  # noqa: E402
from src.config import load_mapping, load_settings, resolve_path  # noqa: E402
from src.data_loader import DatasetError, SparkStartupError, get_spark  # noqa: E402
from src.export_utils import build_report, save_report  # noqa: E402
from src.insights import generate_observations  # noqa: E402
from src.spark_processor import process_dataset  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="AirScope Spark pipeline")
    ap.add_argument("--dataset", help="path to the dataset (default: settings.yaml)")
    ap.add_argument("--force", action="store_true", help="ignore cached Parquet and re-clean")
    args = ap.parse_args()

    settings, mapping = load_settings(), load_mapping()
    path = resolve_path(args.dataset or settings["dataset"]["path"])
    acfg = settings.get("analysis", {})
    try:
        spark = get_spark(settings)
        p = process_dataset(spark, path, settings, mapping, force=args.force)
    except (SparkStartupError, DatasetError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    r = p.report
    rows = r["rows"]
    print(f"\nDataset          : {path.name}  ({r['file_format']}, delimiter={r['csv_delimiter']!r}, "
          f"decimal={r['decimal_separator']!r})")
    print(f"Rows             : loaded {rows['loaded']:,} | blank {rows['blank_rows_removed']:,} | "
          f"no timestamp {rows['excluded_without_timestamp']:,} | no valid measurement "
          f"{rows['excluded_without_any_valid_measurement']:,} | duplicates {rows['exact_duplicates_removed']:,} "
          f"| retained {rows['retained']:,}")
    print(f"Period           : {r['period']['start']} -> {r['period']['end']}")
    print(f"Locations        : {r['n_locations']}  (location column: {r['has_location']})")
    print(f"Parquet          : {'re-used' if p.from_cache else 'written'} -> {p.output_dir}")
    for w in r["column_mapping"]["warnings"] + p.warnings:
        print(f"Note             : {w}")
    if rows["retained"] == 0:
        print("No usable rows — check config/column_mapping.yaml.")
        return 2

    df = p.df.cache()
    meta = p.measurements
    keys = [k for k, m in meta.items() if m["valid_in_final_dataset"] > 0]
    pollutants = [k for k in keys if meta[k]["kind"] in ("pollutant", "aqi")] or keys
    out_dir = resolve_path(settings["paths"]["reports_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = A.summary_stats(df, keys, acfg.get("percentile_accuracy", 10000))
    stats.insert(1, "unit", stats["measurement"].map(lambda k: meta[k]["unit"]))
    stats.to_csv(out_dir / "descriptive_statistics.csv", index=False)
    print("\nDescriptive statistics (valid readings only):")
    print(stats.round(3).to_string(index=False))

    ov = A.overview(df)
    corr, nobs = A.correlation_matrix(df, pollutants, acfg.get("min_correlation_obs", 30))
    corr.round(4).to_csv(out_dir / "correlation_matrix.csv")
    results, all_obs = {"descriptive_statistics": stats, "correlation_matrix": corr.reset_index()}, {}
    for k in pollutants:
        lbl = meta[k]["label"]
        res = {"overview": ov}
        if p.has_time:
            res["monthly"] = A.time_series(df, k, "month")
            res["hourly"] = A.profile(df, k, "hour")
            res["seasonal"] = A.profile(df, k, "season", acfg.get("hemisphere", "north"))
            res["week"] = A.weekday_vs_weekend(df, k)
            res["top_days"] = A.top_periods(df, k, "day", acfg.get("top_n", 10))
            res["monthly"].to_csv(out_dir / f"monthly_{k}.csv", index=False)
        res["outliers"] = A.outlier_analysis(df, k, acfg.get("outlier_method", "iqr"),
                                             float(acfg.get("iqr_multiplier", 1.5)),
                                             float(acfg.get("percentile_threshold", 0.99)),
                                             acfg.get("percentile_accuracy", 10000), acfg.get("top_n", 10))
        if p.has_location:
            res["locations"] = A.location_comparison(df, k)
        st = stats.set_index("measurement").loc[k]
        mon = res.get("monthly")
        obs = generate_observations(
            label=lbl, unit=meta[k]["unit"], overview=ov, stats=st, monthly=mon, hourly=res.get("hourly"),
            seasonal=res.get("seasonal"), week=res.get("week"), top_days=res.get("top_days"),
            outliers=res["outliers"], corr=corr if len(corr) else None,
            corr_labels={x: meta[x]["label"] for x in meta}, locations=res.get("locations"),
            completeness_pct={meta[x]["label"]: 100 * stats.set_index("measurement").loc[x, "count"] / ov["rows"]
                              for x in pollutants},
            min_month_obs=max(1, int(0.25 * mon["count"].median())) if mon is not None and len(mon) else 1)
        all_obs[lbl] = obs
        res["outliers"] = {a: b for a, b in res["outliers"].items() if a not in ("top", "by_hour")}
        results[k] = res
        print(f"\n{lbl}:")
        for o in obs:
            print(f"  - {o}")

    report = build_report(dataset={"file": path.name, "period": r["period"], "locations": ov["locations"]},
                          processing=r, filters={"start": None, "end": None, "locations": "all"},
                          results=results, observations=[f"[{k}] {o}" for k, v in all_obs.items() for o in v])
    rp = save_report(report, out_dir)
    print(f"\nReports written to {out_dir}  (JSON: {rp.name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
