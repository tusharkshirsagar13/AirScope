"""Visual identity for the dashboard: CSS and a shared Plotly style."""
from __future__ import annotations

import plotly.graph_objects as go

BG = "#F6F4EF"
SURFACE = "#FFFFFF"
BORDER = "#E3DED4"
TEXT = "#24292E"
MUTED = "#6A6F73"
ACCENT = "#2E7D74"
ACCENT_SOFT = "rgba(46,125,116,0.14)"
WARM = "#C47F45"
PALETTE = ["#2E7D74", "#C47F45", "#5A6B8C", "#8C9A55", "#A9505A", "#7C6A8E",
           "#3F9AA0", "#B39A4C", "#6E7F7B", "#D08F7A"]
SEQUENTIAL = [[0.0, "#F4F1EA"], [0.5, "#86BDB4"], [1.0, "#1F5C55"]]
DIVERGING = [[0.0, "#B4605A"], [0.5, "#F5F2EC"], [1.0, "#2E7D74"]]

CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@500&display=swap');
html, body, [class*="css"], .stMarkdown, .stText, button, input, textarea, select {{
  font-family: 'IBM Plex Sans', -apple-system, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
}}
.block-container {{ padding-top: 3.4rem; padding-bottom: 2.5rem; max-width: 1280px; }}
h1, h2, h3 {{ color: {TEXT}; letter-spacing: -0.01em; }}
[data-testid="stSidebar"] {{ background: #EFEBE3; border-right: 1px solid {BORDER}; }}
[data-testid="stSidebar"] .block-container {{ padding-top: 1.2rem; }}

.as-header {{ display:flex; justify-content:space-between; align-items:flex-end; gap:1rem;
  flex-wrap:wrap; border-bottom:1px solid {BORDER}; padding-bottom:0.9rem; margin-bottom:1.1rem; }}
.as-brand {{ display:flex; align-items:center; gap:0.7rem; }}
.as-mark {{ width:38px; height:38px; border-radius:9px; background:{ACCENT}; display:flex;
  align-items:center; justify-content:center; }}
.as-title {{ font-size:1.55rem; font-weight:600; color:{TEXT}; line-height:1.1; margin:0; }}
.as-title span {{ color:{MUTED}; font-weight:400; }}
.as-sub {{ color:{MUTED}; font-size:0.93rem; margin-top:0.2rem; }}
.as-pill {{ font-size:0.8rem; color:{TEXT}; background:{SURFACE}; border:1px solid {BORDER};
  border-radius:999px; padding:0.3rem 0.75rem; white-space:nowrap; }}
.as-pill b {{ color:{ACCENT}; font-weight:600; }}

.as-section {{ display:flex; align-items:baseline; gap:0.6rem; margin:1.7rem 0 0.5rem 0; }}
.as-section .num {{ font-family:'IBM Plex Mono', monospace; font-size:0.75rem; color:{ACCENT};
  border:1px solid {ACCENT}; border-radius:5px; padding:0 0.35rem; }}
.as-section .ttl {{ font-size:1.1rem; font-weight:600; color:{TEXT}; }}
.as-section .hint {{ font-size:0.85rem; color:{MUTED}; }}

.as-kpis {{ display:grid; grid-template-columns:repeat(4, minmax(0,1fr)); gap:0.8rem; }}
@media (max-width: 900px) {{ .as-kpis {{ grid-template-columns:repeat(2, minmax(0,1fr)); }} }}
.as-kpi {{ background:{SURFACE}; border:1px solid {BORDER}; border-radius:10px; padding:0.85rem 1rem;
  box-shadow:0 1px 2px rgba(36,41,46,0.04); }}
.as-kpi .lbl {{ font-size:0.78rem; color:{MUTED}; text-transform:uppercase; letter-spacing:0.04em; }}
.as-kpi .val {{ font-size:1.55rem; font-weight:600; color:{TEXT}; margin-top:0.15rem; }}
.as-kpi .val small {{ font-size:0.85rem; color:{MUTED}; font-weight:400; margin-left:0.2rem; }}
.as-kpi .note {{ font-size:0.78rem; color:{MUTED}; margin-top:0.1rem; }}
.as-kpi.accent {{ border-left:3px solid {ACCENT}; }}

.as-card {{ background:{SURFACE}; border:1px solid {BORDER}; border-radius:10px; padding:0.9rem 1.1rem; }}
.as-obs {{ margin:0; padding-left:1.1rem; }}
.as-obs li {{ margin:0.25rem 0; color:{TEXT}; font-size:0.93rem; }}
.as-note {{ font-size:0.82rem; color:{MUTED}; }}
.as-empty {{ background:{SURFACE}; border:1px dashed {BORDER}; border-radius:10px; padding:1rem 1.2rem;
  color:{MUTED}; font-size:0.92rem; }}
.as-footer {{ margin-top:2.4rem; padding-top:1rem; border-top:1px solid {BORDER}; color:{MUTED};
  font-size:0.8rem; display:grid; grid-template-columns:repeat(auto-fit, minmax(220px,1fr)); gap:0.8rem; }}
.as-footer b {{ color:{TEXT}; font-weight:600; }}
.as-side-title {{ font-weight:600; font-size:1.05rem; color:{TEXT}; }}
.as-side-meta {{ font-size:0.8rem; color:{MUTED}; line-height:1.5; }}
div[data-testid="stExpander"] {{ background:{SURFACE}; border-radius:10px; }}
div[data-testid="stTabs"] button[role="tab"] p {{ font-size:0.9rem; }}
</style>
"""

LOGO_SVG = """<svg width="22" height="22" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
<path d="M3 15c3-4 6-4 9 0s6 4 9 0" stroke="#fff" stroke-width="2" stroke-linecap="round"/>
<path d="M3 9c3-4 6-4 9 0s6 4 9 0" stroke="#fff" stroke-opacity=".6" stroke-width="2" stroke-linecap="round"/>
</svg>"""


def style_fig(fig: go.Figure, height: int = 340, legend: bool = True) -> go.Figure:
    fig.update_layout(
        height=height, template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=SURFACE,
        font=dict(family="IBM Plex Sans, -apple-system, Segoe UI, sans-serif", size=12, color=TEXT),
        margin=dict(l=8, r=8, t=36, b=8), colorway=PALETTE,
        title=dict(font=dict(size=14, color=TEXT), x=0.01, xanchor="left"),
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=BORDER, font=dict(color=TEXT)),
        showlegend=legend,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=11)),
    )
    fig.update_xaxes(gridcolor="#EFEBE4", linecolor=BORDER, zeroline=False)
    fig.update_yaxes(gridcolor="#EFEBE4", linecolor=BORDER, zeroline=False)
    return fig
