"""Deterministic, rule-based observations generated from computed results.

Nothing here is hard-coded about any particular dataset: each sentence is a
template filled with numbers that were calculated by Spark. Wording is kept
descriptive ("higher", "associated with") — never causal.
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd


def fmt(v: Any, digits: int = 3) -> str:
    """Human-friendly number formatting with sensible significant figures."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "n/a"
    v = float(v)
    if v == 0:
        return "0"
    mag = abs(v)
    if mag >= 1000:
        return f"{v:,.0f}"
    if mag >= 100:
        return f"{v:.0f}"
    if mag >= 10:
        return f"{v:.1f}"
    if mag >= 1:
        return f"{v:.2f}"
    return f"{v:.{digits}g}"


def unit_suffix(unit: str | None) -> str:
    if not unit or unit.startswith("as reported") or unit == "sensor response":
        return ""
    return f" {unit}"


def generate_observations(*, label: str, unit: str, overview: dict[str, Any],
                          stats: pd.Series | dict | None = None,
                          monthly: pd.DataFrame | None = None,
                          hourly: pd.DataFrame | None = None,
                          seasonal: pd.DataFrame | None = None,
                          week: dict[str, Any] | None = None,
                          top_days: pd.DataFrame | None = None,
                          outliers: dict[str, Any] | None = None,
                          corr: pd.DataFrame | None = None,
                          corr_labels: dict[str, str] | None = None,
                          locations: pd.DataFrame | None = None,
                          completeness_pct: dict[str, float] | None = None,
                          min_month_obs: int = 1) -> list[str]:
    u = unit_suffix(unit)
    obs: list[str] = []

    rows = overview.get("rows", 0)
    if rows == 0:
        return ["No records match the current filters, so no observations can be made."]
    if overview.get("start") is not None and overview.get("end") is not None:
        days = (overview["end"] - overview["start"]).days + 1
        obs.append(f"{rows:,} records cover {overview['start']:%d %b %Y} – {overview['end']:%d %b %Y} "
                   f"({days:,} day{'' if days == 1 else 's'}).")

    if stats is not None and stats.get("count"):
        obs.append(f"{label}: mean {fmt(stats['mean'])}{u}, median {fmt(stats['median'])}{u}, "
                   f"range {fmt(stats['min'])}–{fmt(stats['max'])}{u} across {int(stats['count']):,} valid readings.")

    if locations is not None and len(locations) >= 2:
        hi, lo = locations.iloc[0], locations.iloc[-1]
        obs.append(f"Highest average {label} among the compared locations: {hi.iloc[0]} ({fmt(hi['mean'])}{u}); "
                   f"lowest: {lo.iloc[0]} ({fmt(lo['mean'])}{u}).")

    if monthly is not None and not monthly.empty:
        m = monthly[monthly["count"] >= min_month_obs]
        if len(m) >= 2:
            top, bot = m.loc[m["mean"].idxmax()], m.loc[m["mean"].idxmin()]
            obs.append(f"Highest monthly average {label}: {top['period']:%b %Y} ({fmt(top['mean'])}{u}); "
                       f"lowest: {bot['period']:%b %Y} ({fmt(bot['mean'])}{u}). "
                       f"Months with fewer than {min_month_obs} readings are not ranked.")

    if seasonal is not None and len(seasonal) >= 2:
        top, bot = seasonal.loc[seasonal["mean"].idxmax()], seasonal.loc[seasonal["mean"].idxmin()]
        if top["mean"] and bot["mean"]:
            obs.append(f"By season, {label} averages highest in {top['label']} ({fmt(top['mean'])}{u}) and lowest in "
                       f"{bot['label']} ({fmt(bot['mean'])}{u}) — {fmt(100 * (top['mean'] / bot['mean'] - 1))}% higher. "
                       "Seasons may include months from different years.")

    if hourly is not None and len(hourly) >= 12:
        top, bot = hourly.loc[hourly["mean"].idxmax()], hourly.loc[hourly["mean"].idxmin()]
        obs.append(f"Time of day: {label} peaks on average at {top['label']} ({fmt(top['mean'])}{u}) and is lowest at "
                   f"{bot['label']} ({fmt(bot['mean'])}{u}).")

    if week and week.get("weekday") and week.get("weekend") and week.get("n_weekend"):
        diff = 100 * (week["weekday"] / week["weekend"] - 1)
        word = "higher" if diff > 0 else "lower"
        obs.append(f"Weekday average {label} ({fmt(week['weekday'])}{u}) is {fmt(abs(diff))}% {word} than the "
                   f"weekend average ({fmt(week['weekend'])}{u}).")

    if top_days is not None and not top_days.empty:
        d = top_days.iloc[0]
        obs.append(f"Highest daily average {label}: {d['period']:%d %b %Y} ({fmt(d['mean'])}{u}, "
                   f"{int(d['count'])} readings).")

    if outliers and outliers.get("n"):
        obs.append(f"{outliers['count']:,} readings ({fmt(outliers['share_pct'])}%) exceed the statistical threshold "
                   f"of {fmt(outliers['threshold'])}{u} ({outliers['method'].upper()} method). These are unusual "
                   "values for this dataset, not official pollution alerts.")

    if corr is not None and corr.shape[0] >= 2:
        labels = corr_labels or {}
        pairs = []
        cols = list(corr.columns)
        for i, a in enumerate(cols):
            for b in cols[i + 1:]:
                v = corr.loc[a, b]
                if pd.notna(v):
                    pairs.append((v, a, b))
        if pairs:
            v, a, b = max(pairs, key=lambda t: abs(t[0]))
            strength = "strong" if abs(v) >= 0.7 else "moderate" if abs(v) >= 0.4 else "weak"
            direction = "positive" if v > 0 else "negative"
            obs.append(f"Strongest correlation among the selected measurements: {labels.get(a, a)} and "
                       f"{labels.get(b, b)} (r = {v:.2f}, {strength} {direction}). Correlation shows the values "
                       "move together; it does not show that one causes the other.")

    if completeness_pct:
        worst = min(completeness_pct.items(), key=lambda kv: kv[1])
        if worst[1] < 50:
            obs.append(f"Data gap: only {fmt(worst[1])}% of records contain a valid {worst[0]} value, so "
                       f"{worst[0]} results are less representative than the others.")
    return obs
