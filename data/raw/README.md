# data/raw — input datasets

Put your air-quality file here. Supported: **CSV** (any common delimiter, `.` or `,`
decimals), **Excel .xlsx**, **JSON / JSON-lines**, **Parquet**. Every file in this
folder appears in the dashboard's *Dataset* dropdown. The default file is set by
`dataset.path` in `config/settings.yaml` — no code changes are needed.

Files in this folder are **never modified**; cleaned output goes to `data/processed/`.
Files uploaded through the dashboard are saved in `data/raw/uploads/`.

## Included dataset: `AirQuality.csv`

| | |
|---|---|
| Source | UCI Machine Learning Repository — *Air Quality* |
| Creator | Saverio De Vito (ENEA) |
| Citation | Vito, S. (2008). Air Quality [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C59K5F |
| Licence | Creative Commons Attribution 4.0 International (CC BY 4.0) — redistributed here unchanged, with attribution |
| Content | Hourly averages from a gas multisensor device and co-located reference analysers at road level in a polluted area of an Italian city |
| File layout | `;` delimiter, `,` decimal separator, `-200` = missing value, two empty trailing columns, 114 fully blank lines at the end |

Columns: `Date` (dd/MM/yyyy), `Time` (HH.mm.ss), `CO(GT)` mg/m³, `PT08.S1(CO)` sensor
response, `NMHC(GT)` µg/m³, `C6H6(GT)` µg/m³, `PT08.S2(NMHC)`, `NOx(GT)` ppb,
`PT08.S3(NOx)`, `NO2(GT)` µg/m³, `PT08.S4(NO2)`, `PT08.S5(O3)`, `T` °C, `RH` %,
`AH` absolute humidity (unit not stated by the source). `(GT)` = ground truth from the
reference analyser; `PT08.*` = raw metal-oxide sensor responses with no physical unit.

The dataset has **no city/station column, no PM2.5/PM10/SO2 and no AQI**. AirScope
reports this and analyses what is present instead of inventing those values.

## Using a different dataset

1. Copy it into this folder (or upload it in the sidebar).
2. If its headers are not recognised, add them to the `aliases` lists in
   `config/column_mapping.yaml` (e.g. add `"pm2_5_ugm3"` to `pm25`).
3. If it uses a missing-value code other than `-200`, edit `dataset.missing_sentinels`
   in `config/settings.yaml`.
