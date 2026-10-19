"""AirScope — Air Quality Intelligence (Streamlit dashboard).

Run from the project root:   streamlit run app.py

Data flow:  raw file -> Spark ingestion & cleaning -> partitioned Parquet ->
            Spark aggregations (filters applied lazily) -> small result tables
            -> Plotly charts.  Raw rows are never pulled into the browser except
            a bounded sample in the record table.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src import analytics as A
from src.config import ConfigError, load_mapping, load_settings, resolve_path, PROJECT_ROOT
from src.data_cleaning import LOC, TS
from src.data_loader import DatasetError, SparkStartupError, get_spark
from src.export_utils import LIMITATIONS, build_report, df_to_csv_bytes, report_to_json_bytes
from src.insights import fmt, generate_observations, unit_suffix
from src.spark_processor import ProcessedDataset, list_datasets, process_dataset
from src.ui_theme import (ACCENT, ACCENT_SOFT, CSS, DIVERGING, LOGO_SVG, PALETTE, SEQUENTIAL, WARM,
                          style_fig)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

st.set_page_config(page_title="AirScope — Air Quality Intelligence", page_icon="🌫️",
                   layout="wide", initial_sidebar_state="auto")
st.markdown(CSS, unsafe_allow_html=True)

KIND_ORDER = {"aqi": 0, "pollutant": 1, "sensor": 2, "meteorological": 3}
KIND_LABEL = {"aqi": "Index", "pollutant": "Pollutants", "sensor": "Sensor responses",
              "meteorological": "Weather"}


# --------------------------------------------------------------------------- #
# Cached resources
# --------------------------------------------------------------------------- #
@st.cache_resource(show_spinner=False)
def spark_session(_settings_key: str):
    return get_spark(load_settings())


@st.cache_resource(show_spinner=False, max_entries=4)
def processed(path_str: str, mtime_ns: int, size: int, force_token: int) -> ProcessedDataset:
    settings, mapping = load_settings(), load_mapping()
    spark = spark_session("default")
    p = process_dataset(spark, Path(path_str), settings, mapping, force=force_token > 0)
    if settings.get("spark", {}).get("cache_clean_data", True) and p.report["rows"]["retained"] > 0:
        p.df = p.df.cache()
    return p


# Every analytical query is cached on (dataset fingerprint, filters, params).
# The leading underscore tells Streamlit not to hash the Spark DataFrame.
@st.cache_data(show_spinner=False, max_entries=256)
def q(name: str, key: tuple, _df, **kw):
    fn = getattr(A, name)
    return fn(_df, **kw)


def section(num: str, title: str, hint: str = "") -> None:
    st.markdown(f"<div class='as-section'><span class='num'>{num}</span><span class='ttl'>{title}</span>"
                f"<span class='hint'>{hint}</span></div>", unsafe_allow_html=True)


def empty_state(msg: str) -> None:
    st.markdown(f"<div class='as-empty'>{msg}</div>", unsafe_allow_html=True)


def axis_title(meta: dict) -> str:
    u = meta.get("unit", "")
    return f"{meta['label']} ({u})" if u and not u.startswith("as reported") else meta["label"]


# --------------------------------------------------------------------------- #
# Configuration & Spark
# --------------------------------------------------------------------------- #
try:
    SETTINGS = load_settings()
    load_mapping()
except ConfigError as exc:
    st.error(f"Configuration problem: {exc}")
    st.stop()

ACFG = SETTINGS.get("analysis", {})
UPCFG = SETTINGS.get("upload", {})

try:
    with st.spinner("Starting Apache Spark (first launch can take ~15 s)…"):
        SPARK = spark_session("default")
except SparkStartupError as exc:
    st.error(str(exc))
    st.stop()

# --------------------------------------------------------------------------- #
# Sidebar — dataset selection & upload
# --------------------------------------------------------------------------- #
with st.sidebar:
    st.markdown(f"<div class='as-side-title'>AirScope</div><div class='as-side-meta'>Air quality analytics · "
                f"Spark {SPARK.version}</div>", unsafe_allow_html=True)
    st.write("")
    files = list_datasets(SETTINGS)
    default_path = resolve_path(SETTINGS["dataset"]["path"])
    options = [str(p.relative_to(PROJECT_ROOT)) if p.is_relative_to(PROJECT_ROOT) else str(p) for p in files]
    default_rel = (str(default_path.relative_to(PROJECT_ROOT)) if default_path.is_relative_to(PROJECT_ROOT)
                   else str(default_path))
    if "dataset_choice" not in st.session_state:
        st.session_state.dataset_choice = default_rel if default_rel in options else (options[0] if options else None)

    st.markdown("**Dataset**")
    if options:
        st.selectbox("Dataset file", options, key="dataset_choice", label_visibility="collapsed")
    with st.expander("Upload a dataset"):
        allowed = UPCFG.get("allowed_extensions", ["csv", "xlsx", "json", "parquet"])
        max_mb = UPCFG.get("max_upload_mb", 200)
        up = st.file_uploader(f"CSV, Excel, JSON or Parquet · up to {max_mb} MB", type=allowed)
        st.caption("Larger files: copy them into `data/raw/` and pick them above.")
        if up is not None:
            ext = Path(up.name).suffix.lower().lstrip(".")
            if ext not in allowed:
                st.error(f"Unsupported file type .{ext}")
            elif up.size > max_mb * 1024 * 1024:
                st.error(f"File is larger than {max_mb} MB.")
            elif st.button("Save and analyse", type="primary", width="stretch"):
                up_dir = resolve_path(SETTINGS["paths"]["upload_dir"])
                up_dir.mkdir(parents=True, exist_ok=True)
                stem = re.sub(r"[^A-Za-z0-9_-]+", "_", Path(up.name).stem)[:60] or "dataset"
                target = up_dir / f"{stem}.{ext}"
                n = 1
                while target.exists():
                    target = up_dir / f"{stem}_{n}.{ext}"
                    n += 1
                target.write_bytes(up.getbuffer())
                st.session_state.dataset_choice = str(target.relative_to(PROJECT_ROOT))
                st.rerun()

choice = st.session_state.get("dataset_choice")
if not choice:
    st.markdown(f"<div class='as-header'><div class='as-brand'><div class='as-mark'>{LOGO_SVG}</div>"
                "<div><div class='as-title'>AirScope <span>— Air Quality Intelligence</span></div></div></div></div>",
                unsafe_allow_html=True)
    empty_state("No dataset found. Copy your air-quality file into <code>data/raw/</code> "
                "(CSV, Excel, JSON or Parquet) or upload one from the sidebar.")
    st.stop()

ds_path = resolve_path(choice)
force_token = st.session_state.get("force_token", 0)
try:
    with st.spinner("Processing dataset with Spark (ingest → clean → Parquet)…"):
        stat = ds_path.stat()
        P = processed(str(ds_path), stat.st_mtime_ns, stat.st_size, force_token)
except (DatasetError, FileNotFoundError) as exc:
    st.error(f"Could not load **{ds_path.name}**: {exc}")
    st.stop()
except Exception as exc:  # noqa: BLE001 — show a friendly message instead of a stack trace
    st.error(f"Unexpected error while processing **{ds_path.name}**: {exc}")
    st.stop()

R = P.report
META = P.measurements
FP = R.get("fingerprint", "x")
retained = R["rows"]["retained"]

# Measurements ordered: AQI, pollutants, sensors, weather
meas_keys = sorted([k for k, m in META.items() if m["valid_in_final_dataset"] > 0],
                   key=lambda k: (KIND_ORDER.get(META[k]["kind"], 9), list(META).index(k)))

# --------------------------------------------------------------------------- #
# Sidebar — filters
# --------------------------------------------------------------------------- #
with st.sidebar:
    st.markdown(f"<div class='as-side-meta'>{retained:,} clean records · "
                f"{len(meas_keys)} measurements<br>"
                + (f"{R['period']['start'][:10]} → {R['period']['end'][:10]}" if R.get("period", {}).get("start") else "")
                + "</div>", unsafe_allow_html=True)
    st.divider()
    st.markdown("**Filters**")
    start = end = None
    locations_sel: list[str] = []
    if P.has_time and retained:
        dmin = pd.Timestamp(R["period"]["start"]).date()
        dmax = pd.Timestamp(R["period"]["end"]).date()
        dr = st.date_input("Date range", value=(dmin, dmax), min_value=dmin, max_value=dmax, format="DD/MM/YYYY")
        if isinstance(dr, (tuple, list)) and len(dr) == 2:
            start, end = dr
        elif isinstance(dr, (tuple, list)) and len(dr) == 1:
            start, end = dr[0], dmax
    if P.has_location and retained:
        all_locs = q("location_comparison", (FP, "all_locs"), P.df, col=meas_keys[0], limit=10000)[LOC].tolist() \
            if meas_keys else []
        locations_sel = st.multiselect("Cities / stations", sorted(all_locs), placeholder="All locations")

    if meas_keys:
        def _fmt(k: str) -> str:
            m = META[k]
            u = unit_suffix(m["unit"]).strip()
            return f"{m['label']} · {u}" if u else m["label"]
        default_sel = next((k for k in meas_keys if META[k]["kind"] in ("aqi", "pollutant")), meas_keys[0])
        sel = st.selectbox("Measurement", meas_keys, index=meas_keys.index(default_sel), format_func=_fmt)
    else:
        sel = None
    gran_label = st.radio("Trend resolution", list(A.GRANULARITIES), index=1, horizontal=True) if P.has_time else None
    report_slot = st.container()
    st.divider()
    st.markdown("**Processing status**")
    status_bits = [
        f"Engine: Apache Spark {SPARK.version} ({SPARK.sparkContext.master})",
        ("Re-used cleaned Parquet (unchanged source)" if P.from_cache
         else f"Cleaned in {R.get('processing_seconds', '?')} s"),
        ("Stored as partitioned Parquet (year/month)" if P.persisted else "Kept in memory (Parquet not written)"),
        f"Rows loaded {R['rows']['loaded']:,} → retained {retained:,}",
    ]
    st.markdown("<div class='as-side-meta'>" + "<br>".join(status_bits) + "</div>", unsafe_allow_html=True)
    if st.button("Re-process dataset", width="stretch",
                 help="Ignore the cached Parquet and run the Spark cleaning job again."):
        st.session_state.force_token = force_token + 1
        st.cache_data.clear()
        st.rerun()

# --------------------------------------------------------------------------- #
# Header
# --------------------------------------------------------------------------- #
period_txt = (f"{start:%d %b %Y} – {end:%d %b %Y}" if start and end else "Full dataset")
st.markdown(
    f"""<div class='as-header'>
      <div class='as-brand'><div class='as-mark'>{LOGO_SVG}</div>
        <div><div class='as-title'>AirScope <span>— Air Quality Intelligence</span></div>
        <div class='as-sub'>Explore pollution patterns. Understand environmental trends.</div></div></div>
      <div class='as-pill'><b>{ds_path.name}</b> · {period_txt}</div>
    </div>""", unsafe_allow_html=True)

for w in P.warnings:
    st.warning(w)
notes = R.get("column_mapping", {}).get("warnings", [])
if notes:
    with st.expander(f"Dataset notes ({len(notes)})", expanded=False):
        for n_ in notes:
            st.markdown(f"- {n_}")
        unm = R.get("column_mapping", {}).get("unmapped", [])
        if unm:
            st.markdown(f"- Unmapped columns (not analysed): {', '.join(unm)}")

if retained == 0 or not sel:
    empty_state("No usable records were found after cleaning. Open <b>Dataset notes</b> above and check "
                "<code>config/column_mapping.yaml</code> so your column names are recognised.")
    with st.expander("Cleaning report"):
        st.json(R)
    st.stop()

# --------------------------------------------------------------------------- #
# Filtered data (lazy Spark plan) & core queries
# --------------------------------------------------------------------------- #
fdf = A.apply_filters(P.df, start, end, locations_sel)
FKEY = (FP, str(start), str(end), tuple(sorted(locations_sel)))
m = META[sel]
U = unit_suffix(m["unit"])
gran = A.GRANULARITIES.get(gran_label, "day") if gran_label else None

with st.spinner("Running Spark aggregations…"):
    ov = q("overview", FKEY, fdf)
    if ov["rows"] == 0:
        empty_state("No records match the selected filters. Widen the date range or clear the location filter.")
        st.stop()
    stats_all = q("summary_stats", FKEY, fdf, cols=meas_keys, accuracy=ACFG.get("percentile_accuracy", 10000))
    st_sel = stats_all.set_index("measurement").loc[sel]

# --------------------------------------------------------------------------- #
# Section 2 — key indicators
# --------------------------------------------------------------------------- #
section("01", "Key indicators", period_txt)
site_lbl = "Cities / stations" if P.has_location else "Monitoring sites"
site_note = "" if P.has_location else "single site — no location column"
cards = [
    ("Records analysed", f"{ov['rows']:,}", f"of {retained:,} clean records", False),
    (site_lbl, f"{ov['locations']:,}", site_note, False),
    (f"Average {m['label']}", f"{fmt(st_sel['mean'])}<small>{U}</small>",
     f"{int(st_sel['count']):,} valid readings", True),
]
if "aqi" in META and META["aqi"]["valid_in_final_dataset"] and sel != "aqi":
    aqi_mean = stats_all.set_index("measurement").loc["aqi", "mean"]
    cards.append(("Average AQI", fmt(aqi_mean), "as supplied by the source (scale not verified)", False))
else:
    cards.append((f"Peak {m['label']}", f"{fmt(st_sel['max'])}<small>{U}</small>",
                  f"median {fmt(st_sel['median'])}{U}", False))
st.markdown("<div class='as-kpis'>" + "".join(
    f"<div class='as-kpi{' accent' if acc else ''}'><div class='lbl'>{l}</div><div class='val'>{v}</div>"
    f"<div class='note'>{n_}</div></div>" for l, v, n_, acc in cards) + "</div>", unsafe_allow_html=True)

# Pre-compute what the observation panel needs
hemi = ACFG.get("hemisphere", "north")
monthly = q("time_series", FKEY, fdf, col=sel, granularity="month") if P.has_time else None
hourly = q("profile", FKEY, fdf, col=sel, by="hour") if P.has_time else None
seasonal = q("profile", FKEY, fdf, col=sel, by="season", hemisphere=hemi) if P.has_time else None
weekly = q("weekday_vs_weekend", FKEY, fdf, col=sel) if P.has_time else None
top_days = q("top_periods", FKEY, fdf, col=sel, granularity="day", n=ACFG.get("top_n", 10)) if P.has_time else None
out = q("outlier_analysis", FKEY, fdf, col=sel, method=ACFG.get("outlier_method", "iqr"),
        k=float(ACFG.get("iqr_multiplier", 1.5)), percentile=float(ACFG.get("percentile_threshold", 0.99)),
        accuracy=ACFG.get("percentile_accuracy", 10000), top_n=ACFG.get("top_n", 10))
locs_df = q("location_comparison", FKEY, fdf, col=sel) if P.has_location else None
corr_default = [k for k in meas_keys if META[k]["kind"] in ("pollutant", "aqi")] or meas_keys
corr, nobs = q("correlation_matrix", FKEY, fdf, cols=corr_default, min_obs=ACFG.get("min_correlation_obs", 30))
label_of = {k: META[k]["label"] for k in META}
compl = {META[k]["label"]: 100.0 * stats_all.set_index("measurement").loc[k, "count"] / ov["rows"]
         for k in corr_default}
min_month_obs = int(0.25 * monthly["count"].median()) if monthly is not None and len(monthly) else 1

observations = generate_observations(
    label=m["label"], unit=m["unit"], overview=ov, stats=st_sel, monthly=monthly, hourly=hourly,
    seasonal=seasonal, week=weekly, top_days=top_days, outliers=out,
    corr=corr if len(corr) else None, corr_labels=label_of, locations=locs_df,
    completeness_pct=compl, min_month_obs=max(1, min_month_obs))

st.write("")
st.markdown("<div class='as-card'><div style='font-weight:600;margin-bottom:0.3rem'>What the data says</div>"
            "<ul class='as-obs'>" + "".join(f"<li>{o}</li>" for o in observations) + "</ul>"
            "<div class='as-note' style='margin-top:0.4rem'>Generated automatically from the computed results "
            "below; recalculated whenever the filters change.</div></div>", unsafe_allow_html=True)

# --------------------------------------------------------------------------- #
# Section 3 — trends
# --------------------------------------------------------------------------- #
section("02", "Pollution trends", f"{m['label']} over time · {gran_label.lower() if gran_label else ''} averages")
if not P.has_time:
    empty_state("The dataset has no recognised date/time column, so trends over time cannot be shown.")
else:
    ts = q("time_series", FKEY, fdf, col=sel, granularity=gran)
    if ts.empty:
        empty_state(f"No valid {m['label']} values in the selected period.")
    else:
        show_band = st.toggle("Show min–max range", value=gran in ("day", "week", "month"))
        fig = go.Figure()
        if show_band:
            fig.add_trace(go.Scatter(x=ts["period"], y=ts["max"], mode="lines", line=dict(width=0),
                                     showlegend=False, hoverinfo="skip"))
            fig.add_trace(go.Scatter(x=ts["period"], y=ts["min"], mode="lines", line=dict(width=0),
                                     fill="tonexty", fillcolor=ACCENT_SOFT, name="min–max range",
                                     hoverinfo="skip"))
        fig.add_trace(go.Scatter(x=ts["period"], y=ts["mean"], mode="lines", name=f"{gran_label} mean",
                                 line=dict(color=ACCENT, width=1.6 if gran in ("hour", "day") else 2.2),
                                 customdata=ts["count"],
                                 hovertemplate="%{x|%d %b %Y %H:%M}<br>mean %{y:.3~f}<br>%{customdata} readings<extra></extra>"))
        fig.update_layout(title=f"{m['name']} — {gran_label.lower()} average", yaxis_title=axis_title(m))
        st.plotly_chart(style_fig(fig, 360), width="stretch")
        st.caption(f"Each point is the mean of the valid readings in that period ({len(ts):,} periods). "
                   "Gaps mean no valid measurement was recorded — missing values are never filled in.")

# --------------------------------------------------------------------------- #
# Section 4 — location comparison
# --------------------------------------------------------------------------- #
section("03", "Location comparison", "average level by city / station" if P.has_location else "")
if P.has_location and locs_df is not None and not locs_df.empty:
    c1, c2 = st.columns([3, 1])
    with c2:
        n_show = st.slider("Locations shown", 3, max(3, min(40, len(locs_df))), min(12, len(locs_df))) \
            if len(locs_df) > 3 else len(locs_df)
        order = st.radio("Show", ["Highest", "Lowest"], horizontal=True)
    sub = locs_df.head(n_show) if order == "Highest" else locs_df.tail(n_show).iloc[::-1]
    with c1:
        fig = px.bar(sub.iloc[::-1], x="mean", y=LOC, orientation="h", custom_data=["count", "max"],
                     color_discrete_sequence=[ACCENT])
        fig.update_traces(hovertemplate="%{y}<br>mean %{x:.3~f}<br>max %{customdata[1]:.3~f}"
                                        "<br>%{customdata[0]} readings<extra></extra>")
        fig.update_layout(title=f"Average {m['label']} by location — {order.lower()} {len(sub)}",
                          xaxis_title=axis_title(m), yaxis_title=None)
        st.plotly_chart(style_fig(fig, max(280, 26 * len(sub) + 80), legend=False), width="stretch")
else:
    c1, c2 = st.columns([1, 2])
    with c1:
        empty_state("This dataset has <b>no city, location or station column</b> — all readings come from a "
                    "single monitoring site, so locations cannot be compared.<br><br>Instead, the chart compares "
                    "the site with itself across the days of the week; the hour-of-day pattern is in section 05.")
    with c2:
        if P.has_time:
            wk = q("profile", FKEY, fdf, col=sel, by="weekday")
            fig = go.Figure()
            fig.add_trace(go.Bar(x=wk["label"], y=wk["mean"], marker_color=[WARM if d >= 5 else ACCENT for d in wk["key"]],
                                 customdata=wk["count"],
                                 hovertemplate="%{x}<br>mean %{y:.3~f}<br>%{customdata} readings<extra></extra>"))
            fig.update_layout(title=f"Average {m['label']} by day of week (weekend in amber)",
                              yaxis_title=axis_title(m))
            st.plotly_chart(style_fig(fig, 300, legend=False), width="stretch")

# --------------------------------------------------------------------------- #
# Section 5 — pollutant analysis
# --------------------------------------------------------------------------- #
section("04", "Pollutant analysis", "relative movement and descriptive statistics")
pol_keys = [k for k in meas_keys if META[k]["kind"] in ("pollutant", "aqi")]
if len(pol_keys) >= 1 and P.has_time:
    chosen = st.multiselect("Compare", pol_keys, default=pol_keys, format_func=lambda k: META[k]["label"])
    if chosen:
        idx = q("indexed_monthly", FKEY, fdf, cols=chosen)
        if not idx.empty:
            idx["label"] = idx["measurement"].map(label_of)
            fig = px.line(idx, x="period", y="index", color="label", markers=True,
                          custom_data=["mean", "count"], color_discrete_sequence=PALETTE)
            fig.update_traces(hovertemplate="%{x|%b %Y}<br>index %{y:.0f}<br>mean %{customdata[0]:.3~f}"
                                            "<br>%{customdata[1]} readings<extra></extra>")
            fig.add_hline(y=100, line_dash="dot", line_color="#9AA0A3")
            fig.update_layout(title="Monthly average relative to each pollutant's own overall mean (=100)",
                              yaxis_title="Index (overall mean = 100)", xaxis_title=None, legend_title_text="")
            st.plotly_chart(style_fig(fig, 360), width="stretch")
            st.caption("Pollutants are measured in different units (e.g. mg/m³, µg/m³, ppb), so their raw values "
                       "are not plotted on one axis. Indexing shows only how each one rises or falls relative to "
                       "its own average. Months with sparse data (see Data quality) are less reliable.")

tbl = stats_all.copy()
tbl.insert(1, "Unit", tbl["measurement"].map(lambda k: META[k]["unit"]))
tbl.insert(2, "Type", tbl["measurement"].map(lambda k: KIND_LABEL.get(META[k]["kind"], META[k]["kind"])))
tbl["measurement"] = tbl["measurement"].map(label_of)
tbl["Valid %"] = (100.0 * tbl["count"] / ov["rows"]).round(1)
tbl = tbl.rename(columns={"measurement": "Measurement", "count": "Valid readings", "mean": "Mean", "std": "Std dev",
                          "min": "Min", "q1": "Q1", "median": "Median", "q3": "Q3", "max": "Max"})
st.dataframe(tbl, hide_index=True, width="stretch",
             column_config={c: st.column_config.NumberColumn(format="%.3g") for c in
                            ["Mean", "Std dev", "Min", "Q1", "Median", "Q3", "Max"]})
st.caption("Quartiles and median use Spark's percentile_approx (approximate, accuracy set in settings.yaml). "
           "Sensor responses have no physical unit and are uncalibrated.")

# --------------------------------------------------------------------------- #
# Section 6 — additional insights
# --------------------------------------------------------------------------- #
section("05", "Additional insights")
tabs = st.tabs(["Daily & seasonal patterns", "Distribution", "Correlation", "High readings",
                "Data quality", "Spark vs Pandas"])

with tabs[0]:
    if not P.has_time:
        empty_state("Needs a date/time column.")
    else:
        c1, c2 = st.columns(2)
        with c1:
            fig = go.Figure(go.Scatter(x=hourly["label"], y=hourly["mean"], mode="lines+markers",
                                       line=dict(color=ACCENT, width=2), customdata=hourly["count"],
                                       hovertemplate="%{x}<br>mean %{y:.3~f}<br>%{customdata} readings<extra></extra>"))
            fig.update_layout(title=f"Average {m['label']} by hour of day", yaxis_title=axis_title(m))
            fig.update_xaxes(tickmode="array", tickvals=list(hourly["label"])[::3], tickangle=0)
            st.plotly_chart(style_fig(fig, 300, legend=False), width="stretch")
        with c2:
            mon = q("profile", FKEY, fdf, col=sel, by="month")
            fig = go.Figure(go.Bar(x=mon["label"], y=mon["mean"], marker_color=ACCENT, customdata=mon["count"],
                                   hovertemplate="%{x}<br>mean %{y:.3~f}<br>%{customdata} readings<extra></extra>"))
            fig.update_layout(title=f"Average {m['label']} by calendar month", yaxis_title=axis_title(m))
            st.plotly_chart(style_fig(fig, 300, legend=False), width="stretch")
        if seasonal is not None and not seasonal.empty:
            s_tbl = seasonal[["label", "months", "mean", "count"]].rename(
                columns={"label": "Season", "months": "Months in data", "mean": f"Mean{U}", "count": "Readings"})
            st.dataframe(s_tbl, hide_index=True, width="stretch",
                         column_config={f"Mean{U}": st.column_config.NumberColumn(format="%.3g")})
            st.caption(f"Meteorological seasons for the {hemi}ern hemisphere (configurable). Calendar-month and "
                       "season averages may pool data from different years.")

with tabs[1]:
    hist = q("histogram", FKEY, fdf, col=sel, bins=int(ACFG.get("histogram_bins", 40)))
    if hist.empty:
        empty_state("No valid values to plot.")
    else:
        fig = go.Figure(go.Bar(x=(hist["bin_start"] + hist["bin_end"]) / 2, y=hist["count"],
                               width=(hist["bin_end"] - hist["bin_start"]) * 0.95, marker_color=ACCENT,
                               customdata=hist[["bin_start", "bin_end"]],
                               hovertemplate="%{customdata[0]:.3~f} – %{customdata[1]:.3~f}<br>%{y} readings<extra></extra>"))
        for v, nm, col, pos in [(st_sel["median"], "median", "#24292E", "top left"),
                                (st_sel["mean"], "mean", WARM, "top right")]:
            fig.add_vline(x=v, line_dash="dash", line_color=col, annotation_text=nm, annotation_position=pos)
        fig.update_layout(title=f"Distribution of {m['label']} readings", xaxis_title=axis_title(m),
                          yaxis_title="Readings", bargap=0)
        st.plotly_chart(style_fig(fig, 330, legend=False), width="stretch")
        skew = "right-skewed (a long tail of high readings)" if st_sel["mean"] > st_sel["median"] * 1.05 else \
            "roughly symmetric" if abs(st_sel["mean"] - st_sel["median"]) <= 0.05 * abs(st_sel["median"] or 1) else "left-skewed"
        st.caption(f"Histogram computed in Spark with {len(hist)} equal-width bins. Mean {fmt(st_sel['mean'])} vs "
                   f"median {fmt(st_sel['median'])}: the distribution is {skew}.")

with tabs[2]:
    corr_cols = st.multiselect("Measurements in the matrix", meas_keys, default=corr_default,
                               format_func=lambda k: META[k]["label"], key="corr_cols")
    if len(corr_cols) < 2:
        empty_state("Select at least two measurements.")
    else:
        cm, cn = q("correlation_matrix", FKEY, fdf, cols=corr_cols, min_obs=ACFG.get("min_correlation_obs", 30))
        labels = [label_of[c] for c in corr_cols]
        z = cm.values
        text = [[("–" if pd.isna(v) else f"{v:.2f}") for v in row] for row in z]
        fig = go.Figure(go.Heatmap(z=z, x=labels, y=labels, zmin=-1, zmax=1, colorscale=DIVERGING,
                                   text=text, texttemplate="%{text}", customdata=cn.values,
                                   hovertemplate="%{y} × %{x}<br>r = %{z:.3f}<br>n = %{customdata}<extra></extra>",
                                   colorbar=dict(thickness=10, title="r")))
        fig.update_layout(title="Pearson correlation (pairwise-complete observations)")
        fig.update_yaxes(autorange="reversed")
        st.plotly_chart(style_fig(fig, 120 + 42 * len(corr_cols), legend=False), width="stretch")
        st.caption(f"Each pair uses only hours where both values are valid (n shown on hover); pairs with fewer "
                   f"than {ACFG.get('min_correlation_obs', 30)} joint readings are left blank. Correlation means two "
                   "measurements tend to move together — it does not establish that one causes the other "
                   "(both may share a common driver such as traffic or weather).")

with tabs[3]:
    if not out.get("n"):
        empty_state("No valid values.")
    else:
        st.markdown(f"<div class='as-note'>Rule: <b>{out['rule']}</b> → <b>{out['count']:,}</b> of "
                    f"{out['n']:,} readings ({fmt(out['share_pct'])}%) flagged. These are statistically unusual "
                    "values for this dataset — <b>not</b> official pollution warnings, which depend on legal limits "
                    "and averaging periods for each jurisdiction.</div>", unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            bm = out["by_month"]
            if bm is not None and not bm.empty:
                fig = go.Figure(go.Bar(x=bm["period"], y=bm["share_pct"], marker_color=WARM,
                                       customdata=bm[["outliers", "observations"]],
                                       hovertemplate="%{x|%b %Y}<br>%{y:.1f}% of readings<br>"
                                                     "%{customdata[0]} of %{customdata[1]}<extra></extra>"))
                fig.update_layout(title="Share of readings above the threshold, by month", yaxis_title="% of readings")
                st.plotly_chart(style_fig(fig, 300, legend=False), width="stretch")
        with c2:
            if top_days is not None and not top_days.empty:
                td = top_days[["period", "mean", "max", "count"]].rename(columns={
                    "period": "Day", "mean": f"Daily mean{U}", "max": f"Hourly max{U}", "count": "Readings"})
                td["Day"] = pd.to_datetime(td["Day"]).dt.strftime("%d %b %Y")
                st.markdown(f"**Highest-{m['label']} days**")
                st.dataframe(td, hide_index=True, width="stretch",
                             column_config={c: st.column_config.NumberColumn(format="%.3g")
                                            for c in [f"Daily mean{U}", f"Hourly max{U}"]})
                st.caption(f"Days need at least {int(top_days['min_obs_required'].iloc[0])} readings "
                           "(75% of a full day of data) to be ranked.")
        if not out["top"].empty:
            st.markdown("**Highest individual readings**")
            st.dataframe(out["top"].rename(columns={TS: "Timestamp", LOC: "Location", sel: f"{m['label']}{U}"}),
                         hide_index=True, width="stretch")

with tabs[4]:
    rows = R["rows"]
    funnel = pd.DataFrame([
        ("Rows in source file", rows["loaded"]),
        ("Blank rows removed", -rows["blank_rows_removed"]),
        ("Excluded: no parseable timestamp", -rows["excluded_without_timestamp"]),
        ("Excluded: no valid measurement at all", -rows["excluded_without_any_valid_measurement"]),
        ("Exact duplicates removed", -rows["exact_duplicates_removed"]),
        ("Rows retained", rows["retained"]),
    ], columns=["Step", "Rows"])
    c1, c2 = st.columns([2, 3])
    with c1:
        st.markdown("**Row-level cleaning**")
        st.dataframe(funnel, hide_index=True, width="stretch")
        ts_info = R.get("timestamp", {})
        st.caption(f"Delimiter `{R.get('csv_delimiter') or '-'}`, decimal `{R.get('decimal_separator')}`, "
                   f"missing-value code(s) {R.get('missing_sentinels')}, timestamp format "
                   f"`{ts_info.get('detected_format')}`. The original file is never modified.")
    with c2:
        st.markdown("**Value-level validation per measurement**")
        vt = pd.DataFrame([{
            "Measurement": v["label"], "Source column": v["source_column"], "Unit": v["unit"],
            "Valid": v["valid"], "Missing code": v["sentinel"], "Empty": v["empty"],
            "Non-numeric": v["unparseable"], "Out of range": v["below_min"] + v["above_max"],
            "Valid % (retained rows)": v["pct_valid_in_final_dataset"]} for v in META.values()])
        st.dataframe(vt, hide_index=True, width="stretch")
    if P.has_time:
        comp = q("completeness", FKEY, fdf, cols=meas_keys)
        if not comp.empty:
            zc = comp[meas_keys].T.values
            fig = go.Figure(go.Heatmap(z=zc, x=comp["period"], y=[label_of[k] for k in meas_keys],
                                       colorscale=SEQUENTIAL, zmin=0, zmax=100, colorbar=dict(thickness=10, title="%"),
                                       hovertemplate="%{y} · %{x|%b %Y}<br>%{z:.1f}% valid<extra></extra>"))
            fig.update_layout(title="Valid-value coverage by month (% of rows)")
            fig.update_yaxes(autorange="reversed")
            st.plotly_chart(style_fig(fig, 110 + 24 * len(meas_keys), legend=False), width="stretch")
    with st.expander("Full cleaning report (JSON)"):
        st.json(R, expanded=False)

with tabs[5]:
    st.markdown("<div class='as-note'>Times the <b>same</b> job — read the cleaned Parquet, compute the monthly "
                "mean of every measurement, collect the result — in Pandas and in Spark (warm-up run excluded, "
                "median of 3). Spark here runs in <b>local mode on one machine</b>; that is not a cluster test, and "
                "on small data Spark's scheduling overhead usually makes it slower.</div>", unsafe_allow_html=True)
    if not P.persisted or not P.has_time:
        empty_state("The benchmark needs the cleaned Parquet output and a timestamp column.")
    else:
        scale = st.select_slider("Scale factor (rows replicated for timing only)", [1, 5, 20, 50], value=1)
        if st.button("Run benchmark"):
            from src.benchmark import run_benchmark
            with st.spinner("Benchmarking…"):
                b = run_benchmark(SPARK, P.output_dir / "clean.parquet", meas_keys, repeats=3, scale=scale)
            st.session_state["bench"] = b
        b = st.session_state.get("bench")
        if b:
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows", f"{b['rows']:,}", f"×{b['scale_factor']}" if b["scale_factor"] > 1 else None)
            c2.metric("Pandas (median)", f"{b['pandas_median_s']:.3f} s")
            c3.metric("Spark (median)", f"{b['spark_median_s']:.3f} s")
            st.caption(f"Results equivalent: {b['results_equivalent']} · input {b['input_size_mb']} MB · "
                       f"{b['environment']['system']}, {b['environment']['cpu_count']} CPUs, Spark "
                       f"{b['environment']['spark']} ({b['environment']['spark_master']}). {b['caveat']}")

# --------------------------------------------------------------------------- #
# Section 7 — data table
# --------------------------------------------------------------------------- #
section("06", "Data explorer", "bounded samples and aggregated results")
t1, t2 = st.tabs(["Records", "Aggregated results"])
with t1:
    max_rows = int(ACFG.get("max_table_rows", 1000))
    c1, c2 = st.columns([3, 1])
    with c1:
        cols_show = st.multiselect("Columns", meas_keys, default=[sel] + [k for k in corr_default if k != sel][:4],
                                   format_func=lambda k: META[k]["label"], key="tbl_cols")
    with c2:
        only_valid = st.toggle(f"Only rows with {m['label']}", value=False)
    src_df = fdf.filter(fdf[sel].isNotNull()) if only_valid else fdf
    sample = q("sample_records", FKEY + (only_valid,), src_df, cols=cols_show or [sel], limit=max_rows)
    search = st.text_input("Search in table", placeholder="e.g. 2004-11-23 or a location name")
    view = sample
    if search:
        mask = sample.astype(str).apply(lambda col: col.str.contains(search, case=False, regex=False)).any(axis=1)
        view = sample[mask]
    st.dataframe(view.rename(columns={**label_of, TS: "Timestamp", LOC: "Location"}), hide_index=True,
                 width="stretch", height=320)
    st.caption(f"Showing {len(view):,} of the {min(max_rows, ov['rows']):,} most recent rows in the selection "
               f"(limit {max_rows:,}; the full data stays in Spark). Blank/None cells are missing or invalid values (never filled in).")
    st.download_button("Download these rows (CSV)", df_to_csv_bytes(view), file_name="airscope_records.csv",
                       mime="text/csv")
with t2:
    agg_choice = st.radio("Table", ["Daily summary", "Monthly summary", "Descriptive statistics"]
                          + (["By location"] if P.has_location else []), horizontal=True)
    if agg_choice == "Daily summary" and P.has_time:
        agg = q("time_series", FKEY, fdf, col=sel, granularity="day")
    elif agg_choice == "Monthly summary" and P.has_time:
        agg = monthly
    elif agg_choice == "By location":
        agg = locs_df
    else:
        agg = stats_all.assign(unit=stats_all["measurement"].map(lambda k: META[k]["unit"]))
    if agg is None or agg.empty:
        empty_state("Not available for this dataset.")
    else:
        st.dataframe(agg, hide_index=True, width="stretch", height=300)
        st.download_button("Download table (CSV)", df_to_csv_bytes(agg),
                           file_name=f"airscope_{agg_choice.lower().replace(' ', '_')}_{sel}.csv", mime="text/csv")

# --------------------------------------------------------------------------- #
# JSON report (sidebar)
# --------------------------------------------------------------------------- #
report = build_report(
    dataset={"file": ds_path.name, "rows_loaded": R["rows"]["loaded"], "rows_retained": retained,
             "period": R.get("period"), "locations": ov["locations"],
             "measurements": {k: {x: META[k][x] for x in ("label", "name", "unit", "kind", "source_column")}
                              for k in meas_keys}},
    processing={"rows": R["rows"], "timestamp": R.get("timestamp"), "spark_version": SPARK.version,
                "per_measurement_validation": {k: {x: META[k][x] for x in
                                                   ("valid", "sentinel", "empty", "unparseable", "below_min", "above_max")}
                                               for k in META}},
    filters={"start": start, "end": end, "locations": locations_sel or "all", "measurement": sel,
             "trend_resolution": gran_label},
    results={"descriptive_statistics": stats_all, "monthly_series": monthly, "hour_of_day_profile": hourly,
             "seasonal_profile": seasonal, "weekday_vs_weekend": weekly, "top_days": top_days,
             "outliers": {k: v for k, v in out.items() if k not in ("top", "by_hour")},
             "correlation_matrix": corr.reset_index().rename(columns={"index": "measurement"}) if len(corr) else None,
             "location_comparison": locs_df},
    observations=observations)
with report_slot:
    st.divider()
    st.markdown("**Analysis report**")
    st.download_button("Download JSON report", report_to_json_bytes(report), file_name="airscope_report.json",
                       mime="application/json", width="stretch",
                       help="Dataset info, processing statistics, filters, results, observations and limitations.")

# --------------------------------------------------------------------------- #
# Footer
# --------------------------------------------------------------------------- #
st.markdown(
    f"""<div class='as-footer'>
      <div><b>AirScope</b><br>Air Quality and Pollution Data Analytics Using Big Data Processing</div>
      <div><b>Stack</b><br>Apache Spark {SPARK.version} (PySpark) · Parquet · Streamlit · Plotly · Python</div>
      <div><b>Dataset</b><br>{ds_path.name} · {R['rows']['loaded']:,} rows loaded, {retained:,} retained</div>
      <div><b>Method</b><br>Spark ingestion → validation → partitioned Parquet → Spark SQL aggregations.
      Results reflect only the supplied dataset; this is an educational tool, not a regulatory or medical service.</div>
    </div>""", unsafe_allow_html=True)
